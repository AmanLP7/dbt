"""Bridge Bluesky's Jetstream firehose into a Kafka topic.

    python -m ingestion.bluesky.producer --create-topic
    python -m ingestion.bluesky.producer               # until stopped
    python -m ingestion.bluesky.producer --max-events 1000

Jetstream is a public WebSocket: one JSON object per frame, already decoded from
the network's DAG-CBOR, so this process is a bridge rather than a consumer of a
public broker. Frames are produced to `bluesky.jetstream` unchanged.

Delivery is at-least-once, on both hops:

* the Kafka producer is idempotent (`enable.idempotence`, `acks=all`), and the
  reader dedupes on the event's `seq`;
* the upstream position is persisted in `main.jetstream_cursor` and resumption
  is *inclusive* -- the first event replayed is the last one already produced --
  so a crash costs a duplicate, never a gap. That is the structural difference
  from the Wikimedia flow, whose SSE position cannot be resumed at all.

The cursor advances on **delivery**, not on enqueue. `produce()` only hands the
message to librdkafka's local queue, so a cursor taken there would move past
events a broker outage later discards -- a permanent hole nothing downstream can
see. `DeliveryCounter` therefore carries the position of the newest
*acknowledged* message, the position is latched at the first failure, and the
persisted value is the earlier of the two. The cost is that a crash replays the
handful of events still in flight (bounded by the producer's `linger.ms`), which
`INSERT OR IGNORE` absorbs.

Jetstream's `time` is microsecond resolution and several events can share one
microsecond (measured: 3.4% of events fall in a shared microsecond at ~300
events/s), so the stored cursor is that timestamp itself, not timestamp plus
one: the server treats the cursor as inclusive, and `+1` skipped every sibling
of the microsecond it landed in.

Jetstream's own replay window is bounded: measured at ~36.9 h from this machine,
and a cursor older than that is **silently clamped** to the oldest retained
event rather than rejected. `bridge` checks the first replayed event's age
against the cursor it asked for -- but only for a cursor past the window, since
inside it a forward jump is a quiet stretch of the filter, not a clamp -- so a
three-day outage is visible instead of looking like a clean resume.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from functools import partial
from typing import Any, Protocol
from urllib.parse import urlencode

import websockets
from confluent_kafka import Producer

# NewTopic is re-exported from cimpl; pyright flags the documented
# `confluent_kafka.admin` import as private.
from confluent_kafka.admin import AdminClient, NewTopic  # pyright: ignore[reportPrivateImportUsage]
from websockets.typing import Subprotocol

from ingestion.bluesky._common import (
    CLAMP_TOLERANCE_SECONDS,
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_COLLECTIONS,
    DEFAULT_KINDS,
    DEFAULT_PING_INTERVAL,
    DEFAULT_PING_TIMEOUT,
    DEFAULT_RECONNECT_SECONDS,
    RETAINED_WINDOW_SECONDS,
    SUBPROTOCOL,
    TOPIC,
    acquire_writer_lock,
    bootstrap_servers,
    configure_logging,
    connect_db,
    database_path,
    ensure_schema,
    event_kind,
    frame_error,
    jetstream_url,
    logger,
    parse_event,
    record_key,
    save_cursor,
    stored_cursor,
)

# The frames are 687 B measured, but a post with an embed carries the whole
# quoted record; 8 MiB is far above anything the firehose sends and stops a
# single frame from closing the socket.
MAX_FRAME_BYTES = 8 * 1024 * 1024


class DeliveryCounter:
    """Tracks which upstream positions librdkafka actually acknowledged.

    `produce()` returns as soon as a message is queued, so the position of the
    last *enqueued* event says nothing about what the broker holds. The delivery
    callback is the only place that knows, and `cursor()` is what the bridge
    persists and reconnects from:

    * it is the position of the newest acknowledged event, so a crash replays
      the in-flight window instead of skipping it;
    * it is latched at the *first* failure, so one rejected message pins the
      cursor there instead of letting later successes step over the hole;
    * it is None until the first acknowledgement, which leaves an existing
      stored cursor untouched rather than overwriting it with nothing.

    Positions are the event's own `time` in epoch microseconds. A message whose
    event carries no parseable `time` reports None and cannot move the cursor.
    """

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self.acked: int | None = None
        self.failed_at: int | None = None

    def callback(self, seen_us: int | None, error, _message) -> None:
        if error is not None:
            self.failed += 1
            if seen_us is not None and (self.failed_at is None or seen_us < self.failed_at):
                self.failed_at = seen_us
            logger.error("Delivery failed at %s: %s", seen_us, error)
            return None
        self.delivered += 1
        if seen_us is not None and (self.acked is None or seen_us > self.acked):
            self.acked = seen_us
        return None

    def cursor(self) -> int | None:
        """The position that is safe to resume from, or None if nothing is safe yet."""
        if self.failed_at is None:
            return self.acked
        if self.acked is None:
            return self.failed_at
        return min(self.acked, self.failed_at)


def build_url(
    base: str,
    *,
    cursor: int | None = None,
    collections: tuple[str, ...] = DEFAULT_COLLECTIONS,
    kinds: tuple[str, ...] = DEFAULT_KINDS,
) -> str:
    """The subscription URL, with filters and a resume position when given.

    v2 repeats `collections=` and `kinds=` per value rather than taking a
    comma-separated list, and rejects more than 100 collections or 10,000 DIDs
    *before* the upgrade -- a bad filter fails the handshake, not the stream.
    A `collections` filter narrows `commit` events only; `account` and `identity`
    carry no collection, so they keep flowing unless `kinds` excludes them.

    `cursor` is Jetstream's `seq` (monotonic, small) or a microsecond timestamp
    (the same unit `payload.time` uses); both were verified to work. This flow
    stores the timestamp form -- see `event_time_us` for why.
    """
    params: list[tuple[str, str]] = []
    if cursor is not None:
        params.append(("cursor", str(cursor)))
    params.extend(("collections", name) for name in collections)
    params.extend(("kinds", kind) for kind in kinds)
    query = urlencode(params)
    return f"{base}?{query}" if query else base


def project(frame: str | bytes) -> tuple[str, dict] | None:
    """Return `(body_json, payload)` for a frame, or None when it must be dropped.

    The body is the upstream payload with two fields added: `kind` (which v2
    spells only as the `#`-suffixed `$type`) and `record_key` (the `at://`
    dedupe key). Both are derived here rather than in SQL because neither is
    expressible in Flink SQL -- there is no right-split of a string, and the key
    is three nullable columns joined with separators.

    Nothing upstream is removed: `$type`, `seq`, `time` and the rest are still
    in the body verbatim, so a reader that wants the original sees it the way
    Jetstream sent it.
    """
    payload = parse_event(frame)
    if payload is None:
        return None
    body = json.dumps(
        {**payload, "kind": event_kind(payload), "record_key": record_key(payload)},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return body, payload


def event_time_us(payload: dict) -> int | None:
    """The event's own `time` as epoch microseconds, or None when unparseable.

    `time` is RFC 3339 with a `Z` suffix. `datetime.fromisoformat` only accepts
    `Z` from Python 3.11, and this tree targets 3.10, so the suffix is rewritten
    rather than parsed by a third-party library.

    Microseconds, not `seq`: Jetstream accepts either as a cursor, and only a
    time cursor lets the producer tell a real replay from a clamped one -- a
    `seq` cursor has no clock to compare the served event against, and a
    collection filter makes `seq` gaps normal rather than diagnostic.
    """
    stamp = payload.get("time")
    if not isinstance(stamp, str):
        return None
    try:
        parsed = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return int(parsed.timestamp() * 1_000_000)


def frame_age_seconds(payload: dict, now: float | None = None) -> float | None:
    """How old the event is, from its own `time`; None when unparseable."""
    stamp_us = event_time_us(payload)
    if stamp_us is None:
        return None
    return (now if now is not None else time.time()) - stamp_us / 1_000_000


def create_topic(bootstrap: str, topic: str = TOPIC, partitions: int = 1, replication: int = 1) -> None:
    """Create the topic if it does not exist; already-exists is not an error."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        logger.info("Topic %s already exists.", topic)
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
    for name, future in futures.items():
        try:
            future.result()
            logger.info("Created topic %s.", name)
        except Exception as exc:  # TopicAlreadyExistsError and friends
            if "already exists" not in str(exc).lower():
                raise


def build_producer(bootstrap: str) -> Producer:
    """An idempotent producer that waits for all replicas.

    Idempotence rules out broker-side duplicates within a producer session and
    forces `acks=all`; across restarts the sink's `INSERT OR IGNORE` is what
    handles replays.
    """
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 200,
            "compression.type": "lz4",
            # 300 msg/s of 687 B frames; the default 32 MiB queue would block
            # `produce` on a slow broker and the WebSocket would back up instead.
            "queue.buffering.max.messages": 200_000,
        }
    )


class ProducerLike(Protocol):
    """The three methods `bridge` calls; a real `Producer` satisfies this structurally."""

    # `key` and `on_delivery` are keyword-only here because librdkafka puts
    # `partition`/`callback` between them positionally.
    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        *,
        key: bytes | None = None,
        on_delivery: Callable[..., None] | None = None,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class Connect(Protocol):
    """One subscription: what `bridge` enters with `async with`.

    Narrowed to a protocol so tests can hand in a scripted socket instead of a
    real WebSocket, without the test module importing `websockets`.
    """

    def __call__(self, url: str) -> AbstractAsyncContextManager[Any]: ...


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[websockets.ClientConnection]:
    """Open one subscription.

    The subprotocol is mandatory: the server rejects the upgrade without it.
    Ping settings are the client's default cadence with a much longer timeout --
    a silent probe of the live stream died at 39 s on `keepalive ping timeout`,
    and a false disconnect costs a reconnect and a handful of replayed events.
    """
    async with websockets.connect(
        url,
        subprotocols=[Subprotocol(SUBPROTOCOL)],
        max_size=MAX_FRAME_BYTES,
        ping_interval=DEFAULT_PING_INTERVAL,
        ping_timeout=DEFAULT_PING_TIMEOUT,
        # The stream is public; identify the client the way the Wikimedia flow
        # does, so an operator reading server logs can tell the flows apart.
        additional_headers={"User-Agent": "dbt_bluesky_pipeline/1.0"},
    ) as socket:
        yield socket


async def bridge(
    producer: ProducerLike,
    connect: Connect = _connect,
    *,
    base_url: str | None = None,
    cursor: int | None = None,
    collections: tuple[str, ...] = DEFAULT_COLLECTIONS,
    kinds: tuple[str, ...] = DEFAULT_KINDS,
    max_events: int | None = None,
    reconnect_seconds: float = DEFAULT_RECONNECT_SECONDS,
    backoffs: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
    on_cursor: Callable[[int], None] | None = None,
    flush_every: int = 500,
    should_stop: Callable[[], bool] = lambda: False,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, int]:
    """Read the firehose and produce each accepted event until stopped.

    `cursor` is where to resume: the `time` of the last event delivered to the
    broker, in epoch microseconds, or None for a live start. `on_cursor` is
    called with the newest such position, throttled to `flush_every` events --
    per-event persistence at ~300 msg/s would be a SQLite write per message, and
    the position only has to be fresh enough that a crash replays a bounded
    amount.

    The position persisted is never the last event *read*: it comes from
    `DeliveryCounter`, so an event that librdkafka has not acknowledged cannot
    be stepped over. See that class for the two ways that can go wrong.

    `connect`, `should_stop`, `clock` and `sleep` are injected so tests can
    exercise the reconnect and clamp paths without a socket or real time.
    """
    base = base_url if base_url is not None else jetstream_url()
    stats = {"events": 0, "enqueued": 0, "dropped": 0, "errors": 0, "reconnects": 0, "clamped": 0}
    counter = DeliveryCounter()
    backoff_index = 0
    done = False
    since_persist = 0

    def durable() -> int | None:
        """The newest position that is safe to resume from, if there is one yet."""
        position = counter.cursor()
        return position if position is not None else cursor

    try:
        while not done:
            if should_stop():
                break
            url = build_url(base, cursor=cursor, collections=collections, kinds=kinds)
            try:
                async with connect(url) as socket:
                    connected_at = clock()
                    # The position this connection asked for. Clamp detection has
                    # to compare against it, not the cursor of the first
                    # connection: after a reconnect `cursor` is recent, and a
                    # stale comparison would report a clamp on every reconnect.
                    requested = cursor
                    logger.info("Connected to %s (cursor=%s).", base, requested)
                    first_frame = True
                    async for frame in socket:
                        if should_stop() or clock() - connected_at >= reconnect_seconds:
                            break

                        record = project(frame)
                        if record is None:
                            reason = frame_error(frame)
                            if reason is None:
                                stats["dropped"] += 1
                            else:
                                stats["errors"] += 1
                                logger.error("Jetstream refused the subscription: %s", reason)
                            continue
                        body, payload = record

                        if first_frame:
                            first_frame = False
                            # The handshake carried usable data, so this peer is
                            # healthy and the next dropped socket starts the
                            # ladder over. Resetting at connect instead let an
                            # upstream that accepts and immediately closes be
                            # retried once a second, forever.
                            backoff_index = 0
                            if requested is not None and _is_clamped(payload, requested, time.time()):
                                stats["clamped"] += 1
                                logger.error(
                                    "Jetstream clamped the resume cursor %d: the first event served is %.0fs old "
                                    "but the cursor asked for %.0fs, past the ~36 h replay window. Those events are "
                                    "gone from upstream and the sink will never have them. This run continues from "
                                    "the served position and fills the retained window; the gap is not recoverable.",
                                    requested,
                                    frame_age_seconds(payload) or 0.0,
                                    time.time() - requested / 1_000_000,
                                )

                        seen_us = event_time_us(payload)
                        key = record_key(payload)
                        producer.produce(
                            TOPIC,
                            value=body.encode("utf-8"),
                            key=(key or "").encode("utf-8"),
                            # The position travels with the message: only the
                            # delivery callback knows whether it landed, and it
                            # is handed the event's own timestamp so the cursor
                            # it publishes needs no lookup.
                            on_delivery=partial(counter.callback, seen_us),
                        )
                        producer.poll(0)
                        stats["events"] += 1
                        stats["enqueued"] += 1
                        since_persist += 1

                        # Resume from what the broker confirmed, never from what
                        # was merely queued. `durable()` lags by the messages in
                        # flight, which is the direction that costs duplicates
                        # rather than holes.
                        position = durable()
                        if position is not None:
                            cursor = position
                        if on_cursor is not None and position is not None and since_persist >= flush_every:
                            on_cursor(position)
                            since_persist = 0
                        if max_events is not None and stats["events"] >= max_events:
                            done = True
                            break
            except asyncio.CancelledError:
                raise
            except (websockets.WebSocketException, OSError, TimeoutError) as exc:
                delay = backoffs[min(backoff_index, len(backoffs) - 1)]
                backoff_index += 1
                stats["reconnects"] += 1
                logger.warning("Stream dropped (%s: %s); reconnecting in %.0fs.", type(exc).__name__, exc, delay)
                if should_stop():
                    break
                await sleep(delay)
                continue

            if done or should_stop():
                break

            # A clean end is still a reconnect, and an upstream that closes
            # immediately must not be hammered at the first rung of the ladder.
            delay = backoffs[min(backoff_index, len(backoffs) - 1)]
            backoff_index += 1
            stats["reconnects"] += 1
            logger.info("Stream ended; reconnecting in %.0fs.", delay)
            await sleep(delay)
    finally:
        # A crash inside the loop (a full queue raising `BufferError`, an
        # unexpected exception from `produce`) must still flush what is already
        # queued, and whatever did land must reach SQLite: those events were
        # read from the socket and the replay window is finite.
        remaining = producer.flush(30)
        if remaining:
            logger.error("%d messages were not delivered before exit.", remaining)
        # `flush` returning means the callbacks have run; anything still queued
        # has no acknowledgement and therefore cannot move the cursor past
        # itself.
        position = durable()
        if on_cursor is not None and position is not None:
            on_cursor(position)
        stats["delivered"] = counter.delivered
        stats["failed"] = counter.failed
        logger.info(
            "Run finished: %d events, %d delivered, %d failed, %d dropped, %d errors, %d reconnects, %d clamped.",
            stats["events"],
            counter.delivered,
            counter.failed,
            stats["dropped"],
            stats["errors"],
            stats["reconnects"],
            stats["clamped"],
        )
    return stats


def _is_clamped(payload: dict, requested_cursor: int, now: float) -> bool:
    """True when the server served a newer position than the cursor asked for.

    Past the replay window Jetstream **silently** returns the oldest retained
    event rather than rejecting the cursor. Measured from this machine on
    2026-10-27: 25 h back replayed exactly (first event age 90,001 s for a
    90,000 s request), 72 h back came back at 132,742 s -- the clamp point,
    ~36.9 h. Without this check a three-day outage would look like a clean
    resume.

    The test is the age of the first frame against the age the cursor implies.
    A served event *older* than requested is a real replay; much *newer* is a
    clamp. `CLAMP_TOLERANCE_SECONDS` absorbs the sub-second ordering jitter
    between a cursor and the next event after it.

    It only runs for a cursor older than `RETAINED_WINDOW_SECONDS`, because a
    cursor inside the window cannot be clamped: a forward jump there is the
    stream's own quiet stretch -- an 11-minute gap in `app.bsky.labeler.service`
    is a gap in what *matched*, not in what the server kept. Warning about it
    used to advise `--live`, which is the one action that builds a real hole.
    """
    requested_age = now - requested_cursor / 1_000_000
    if requested_age <= RETAINED_WINDOW_SECONDS:
        return False
    age = frame_age_seconds(payload, now)
    if age is None:
        return False
    return requested_age - age > CLAMP_TOLERANCE_SECONDS


def _install_signal_handlers() -> dict[str, bool]:
    """Turn SIGINT/SIGTERM into a stop flag so the producer flushes before exit."""
    import signal

    state = {"stop": False}

    def handler(signum, _frame):
        logger.info("Received signal %s; flushing.", signum)
        state["stop"] = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--create-topic", action="store_true", help="Create the topic if missing, then exit.")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after this many events.")
    parser.add_argument("--db", default=None, help="SQLite path (default: data/bluesky.db).")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Start at the live edge, ignoring the stored cursor (leaves a gap in the sink).",
    )
    parser.add_argument(
        "--collections",
        default=None,
        help="Comma-separated record collections to keep (default: everything, e.g. app.bsky.feed.post).",
    )
    args = parser.parse_args()

    configure_logging()
    bootstrap = bootstrap_servers()
    if args.create_topic:
        create_topic(bootstrap, TOPIC)
        return

    db_path = args.db or database_path()
    acquire_writer_lock(db_path)
    db = connect_db(db_path)
    try:
        ensure_schema(db)
        cursor = None if args.live else stored_cursor(db, TOPIC)
        if cursor is None:
            logger.info("Starting at the live edge; nothing in main.jetstream_cursor to resume from.")
        else:
            logger.info("Resuming Jetstream from main.jetstream_cursor (cursor=%d microseconds).", cursor)

        collections: tuple[str, ...] = DEFAULT_COLLECTIONS
        if args.collections:
            collections = tuple(name.strip() for name in args.collections.split(",") if name.strip())

        producer = build_producer(bootstrap)
        state = _install_signal_handlers()
        asyncio.run(
            bridge(
                producer,
                base_url=jetstream_url(),
                cursor=cursor,
                collections=collections,
                max_events=args.max_events,
                on_cursor=lambda position: save_cursor(db, position, TOPIC),
                should_stop=lambda: state["stop"],
            )
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
