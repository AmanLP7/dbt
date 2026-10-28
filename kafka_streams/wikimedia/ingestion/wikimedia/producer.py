"""Bridge the Wikimedia EventStreams firehose into a Kafka topic.

The public door to Wikimedia's stream is HTTP; the Kafka brokers behind it are
WMF-internal, so this process is the bridge: read NDJSON, drop canaries, produce
the event JSON unchanged to `wikimedia.recentchange`.

    python -m ingestion.wikimedia.producer --create-topic
    python -m ingestion.wikimedia.producer               # until stopped
    python -m ingestion.wikimedia.producer --max-events 1000

Delivery is at-least-once. The producer is idempotent (`enable.idempotence`) and
waits for all in-sync replicas, and the consumer dedupes on `meta.id`, so a
duplicate costs nothing downstream. The SSE/NDJSON position is not committed
anywhere: a restart starts at the live edge, and anything produced before the
crash is already in Kafka.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Iterable, Iterator
from typing import Protocol

import requests
from confluent_kafka import Producer

# NewTopic is re-exported from cimpl; pyright flags the documented
# `confluent_kafka.admin` import as private.
from confluent_kafka.admin import AdminClient, NewTopic  # pyright: ignore[reportPrivateImportUsage]

from ingestion.wikimedia._common import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_RECONNECT_SECONDS,
    STREAM_URL,
    TOPIC,
    USER_AGENT,
    bootstrap_servers,
    configure_logging,
    logger,
    parse_event,
)


class DeliveryCounter:
    """Counts produced and failed deliveries from librdkafka's callbacks."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def callback(self, error, _message) -> None:
        if error is not None:
            self.failed += 1
            logger.error("Delivery failed: %s", error)
        else:
            self.delivered += 1
        return None


def iter_ndjson(lines: Iterable[str | bytes]) -> Iterator[str]:
    """Yield non-empty lines; blank keep-alive lines carry no event.

    `iter_lines(decode_unicode=True)` is typed as `str | bytes` because a
    malformed byte sequence can come back undecoded, so both are accepted here.
    """
    for line in lines:
        text = (line.decode("utf-8", "replace") if isinstance(line, bytes) else line).strip()
        if text:
            yield text


def open_stream(read_timeout: float) -> requests.Response:
    """Open the NDJSON stream.

    `Accept: application/json` is what selects NDJSON; the SSE framing is only
    used when the client asks for `text/event-stream`. The stream is public, but
    Wikimedia asks clients to identify themselves.
    """
    response = requests.get(
        STREAM_URL,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Cache-Control": "no-cache",
        },
        stream=True,
        timeout=read_timeout,
    )
    response.raise_for_status()
    return response


def create_topic(bootstrap: str, topic: str, partitions: int = 1, replication: int = 1) -> None:
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


def project(line: str) -> tuple[str, dict] | None:
    """Return `(raw_json, event)` for a line, or None when it must be dropped."""
    event = parse_event(line)
    if event is None:
        return None
    # Re-serialise nothing: the consumer stores what Wikimedia sent.
    return line, event


def bridge(
    producer: ProducerLike,
    *,
    max_events: int | None = None,
    reconnect_seconds: float = DEFAULT_RECONNECT_SECONDS,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    backoffs: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
    should_stop: Callable[[], bool] = lambda: False,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    """Read the firehose and produce each accepted event until stopped.

    Reconnects after `reconnect_seconds` even on a healthy stream, because the
    server closes the connection at 15 minutes, and with the backoff ladder on
    any transport error. `clock` and `sleep` are injected so tests can exercise
    both paths without real time passing.
    """
    stats = {"events": 0, "enqueued": 0, "dropped": 0, "reconnects": 0}
    counter = DeliveryCounter()
    backoff_index = 0
    done = False

    try:
        while not done:
            if should_stop():
                break
            try:
                response = open_stream(read_timeout)
            except requests.RequestException as exc:
                delay = backoffs[min(backoff_index, len(backoffs) - 1)]
                backoff_index += 1
                stats["reconnects"] += 1
                logger.warning("Stream connect failed (%s); retrying in %.0fs.", exc, delay)
                if should_stop():
                    break
                sleep(delay)
                continue

            backoff_index = 0
            connected_at = clock()
            logger.info("Connected to %s.", STREAM_URL)
            try:
                with response:
                    for line in iter_ndjson(response.iter_lines(decode_unicode=True)):
                        if should_stop() or clock() - connected_at >= reconnect_seconds:
                            break

                        record = project(line)
                        if record is None:
                            stats["dropped"] += 1
                            continue
                        raw, event = record
                        producer.produce(
                            TOPIC,
                            value=raw.encode("utf-8"),
                            key=event["meta"]["id"].encode("utf-8"),
                            on_delivery=counter.callback,
                        )
                        producer.poll(0)
                        stats["events"] += 1
                        stats["enqueued"] += 1
                        if max_events is not None and stats["events"] >= max_events:
                            done = True
                            break
            except requests.RequestException as exc:
                delay = backoffs[min(backoff_index, len(backoffs) - 1)]
                backoff_index += 1
                stats["reconnects"] += 1
                logger.warning("Stream dropped (%s); reconnecting in %.0fs.", exc, delay)
                if should_stop():
                    break
                sleep(delay)
                continue
            finally:
                producer.poll(0)

            if done or should_stop():
                break

            stats["reconnects"] += 1
            logger.info("Stream ended; reconnecting in %.0fs.", backoffs[0])
            sleep(backoffs[0])
    finally:
        # A crash inside the loop (a full queue raising `BufferError`, an
        # unexpected exception from `produce`) must still flush what is already
        # queued: those events were read from the stream and cannot be re-read.
        remaining = producer.flush(30)
        if remaining:
            logger.error("%d messages were not delivered before exit.", remaining)
        stats["delivered"] = counter.delivered
        stats["failed"] = counter.failed
        logger.info(
            "Run finished: %d events, %d delivered, %d failed, %d dropped, %d reconnects.",
            stats["events"],
            counter.delivered,
            counter.failed,
            stats["dropped"],
            stats["reconnects"],
        )
    return stats


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
    args = parser.parse_args()

    configure_logging()
    bootstrap = bootstrap_servers()
    if args.create_topic:
        create_topic(bootstrap, TOPIC)
        return

    producer = build_producer(bootstrap)
    state = _install_signal_handlers()
    bridge(producer, max_events=args.max_events, should_stop=lambda: state["stop"])


if __name__ == "__main__":
    main()
