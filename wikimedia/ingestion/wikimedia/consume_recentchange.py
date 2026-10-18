"""Stream Wikimedia recent changes into the raw table in Databricks.

The speed layer of the lambda architecture: a long-running process that tails the
Wikimedia EventStreams `recentchange` firehose and micro-batches the events into
`workspace.wikimedia.raw_wikimedia`. Importing this module only loads environment
variables; all HTTP and Databricks work happens inside :func:`consume` / :func:`main`.

    python -m ingestion.wikimedia.consume_recentchange               # until stopped
    python -m ingestion.wikimedia.consume_recentchange --max-events 1000

The table is append-only: each flush inserts rows and nothing else. History is
therefore cumulative and immutable, which is what lets the batch layer re-read
the same table and lets a restarted stream — at worst — replay a batch it already
wrote.
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import requests

from ingestion.wikimedia._common import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_FLUSH_EVERY,
    DEFAULT_FLUSH_SECONDS,
    DEFAULT_READ_TIMEOUT,
    RAW_COLUMNS,
    STREAM_URL,
    TABLE_NAME,
    append_rows,
    configure_logging,
    connect,
    ensure_columns,
    ensure_table,
    full_table,
    load_timestamp,
    logger,
    read_checkpoint,
    validate_env,
    write_checkpoint,
)

# Wikimedia asks clients to identify themselves; an anonymous client is liable to
# be throttled. https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
USER_AGENT = "dbt_wikimedia_pipeline/1.0 (https://github.com/amanlp7/dbt-learn)"


@dataclass(frozen=True)
class Frame:
    """One server-sent event: its SSE id, event name, and data payload."""

    event_id: str | None
    event: str | None
    data: str


def iter_sse_frames(lines: Iterable[str]) -> Iterator[Frame | None]:
    """Parse an SSE line stream into frames, yielding None for a heartbeat.

    Follows the EventStreams framing: `field: value` lines accumulate until a
    blank line dispatches the frame, and repeated `data:` lines join with
    newlines. A frame with no data is not an event and is skipped, so a heartbeat
    frame never reaches the table.

    The `:`-prefixed comment line — the stream's heartbeat — is yielded as `None`
    rather than dropped: it is the only thing that arrives while the stream is
    quiet, so it is what tells the caller that time has passed and a partially
    filled batch is due to be flushed.
    """
    event_id: str | None = None
    event: str | None = None
    data_lines: list[str] = []

    for raw_line in lines:
        line = raw_line[:-1] if raw_line.endswith("\r") else raw_line
        if not line:
            if data_lines:
                yield Frame(event_id=event_id, event=event, data="\n".join(data_lines))
            event_id, event, data_lines = None, None, []
            continue
        if line.startswith(":"):
            yield None
            continue

        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "id":
            event_id = value
        elif field == "event":
            event = value
        elif field == "data":
            data_lines.append(value)


def open_stream(last_event_id: str | None, read_timeout: float) -> requests.Response:
    """Open the SSE stream, resuming from `last_event_id` when there is one.

    `Last-Event-ID` is the standard SSE resume header, and EventStreams honours it
    by replaying from the Kafka offset the id names — so a restart after a crash
    picks up where the previous process left off rather than at the live edge.
    """
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache",
    }
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    response = requests.get(STREAM_URL, headers=headers, stream=True, timeout=read_timeout)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response


def payload_for(frame: Frame) -> str:
    """Return the raw payload for `frame`: the event JSON exactly as it arrived.

    The event is stored verbatim — `meta.dt`, `meta.id`, `wiki`, `type`, `title`,
    `user` and the rest stay inside the JSON — so a field no layer knows about yet
    is not lost, and the batch layer can reprocess history under new logic without
    a re-ingest. Raises `json.JSONDecodeError` for a frame that is not valid JSON,
    which the caller drops instead of writing a row every later read would trip on.
    """
    payload = frame.data.strip()
    json.loads(payload)
    return payload


def _backoff(backoffs: tuple[float, ...], index: int) -> float:
    """The delay for the `index`-th consecutive failure, capped at the last step."""
    return backoffs[min(index, len(backoffs) - 1)]


def consume(
    cursor,
    *,
    max_events: int | None = None,
    flush_every: int = DEFAULT_FLUSH_EVERY,
    flush_seconds: float = DEFAULT_FLUSH_SECONDS,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    backoffs: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
    should_stop: Callable[[], bool] = lambda: False,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    """Tail the stream, micro-batching into the raw table until stopped.

    A batch is flushed when it reaches `flush_every` rows or when `flush_seconds`
    have passed, whichever comes first — the heartbeat tick is what makes the
    time-based flush fire on a quiet stream, where no event would otherwise
    arrive to re-check the clock.

    Reconnects on any transport error with the backoff ladder, resuming from the
    last committed event id. `max_events` bounds the run for a smoke test, and
    `should_stop` is polled between events so a signal handler can end the loop
    without losing the buffer. `clock` and `sleep` are injected so a test can
    exercise reconnect and timeout paths without real time passing.
    """
    table = full_table(TABLE_NAME)
    last_event_id = read_checkpoint()
    if last_event_id:
        logger.info("Resuming from checkpoint event id %s.", last_event_id)

    stats = {"events": 0, "rows": 0, "flushes": 0, "reconnects": 0}
    buffer: list[str] = []
    pending_id: str | None = None
    next_flush_at = clock() + flush_seconds
    backoff_index = 0
    done = False

    def drain() -> None:
        """Append the buffer and advance the checkpoint, then reset the flush clock."""
        nonlocal last_event_id, next_flush_at
        if buffer:
            stamped = load_timestamp()
            stats["rows"] += append_rows(cursor, table, RAW_COLUMNS, [(payload, stamped) for payload in buffer])
            # Only after the rows are committed, so the checkpoint can never point
            # past data that is not in the table.
            if pending_id:
                write_checkpoint(pending_id)
                last_event_id = pending_id
            buffer.clear()
            stats["flushes"] += 1
        next_flush_at = clock() + flush_seconds

    while not done:
        if should_stop():
            break

        try:
            response = open_stream(last_event_id, read_timeout)
        except requests.RequestException as exc:
            delay = _backoff(backoffs, backoff_index)
            backoff_index += 1
            stats["reconnects"] += 1
            logger.warning("Stream connect failed (%s); retrying in %.0fs.", exc, delay)
            if should_stop():
                break
            sleep(delay)
            continue

        backoff_index = 0
        logger.info("Connected to %s.", STREAM_URL)
        try:
            with response:
                for frame in iter_sse_frames(response.iter_lines(decode_unicode=True)):
                    if should_stop():
                        done = True
                        break

                    if frame is None:
                        if buffer and clock() >= next_flush_at:
                            drain()
                        continue

                    if frame.event_id:
                        # Held until this frame's batch commits, so the checkpoint
                        # never runs ahead of the table.
                        pending_id = frame.event_id

                    try:
                        buffer.append(payload_for(frame))
                    except json.JSONDecodeError:
                        logger.warning("Dropping non-JSON frame (id=%s).", frame.event_id)
                        continue

                    stats["events"] += 1
                    if len(buffer) >= flush_every or clock() >= next_flush_at:
                        drain()
                    if max_events is not None and stats["events"] >= max_events:
                        done = True
                        break
        except requests.RequestException as exc:
            delay = _backoff(backoffs, backoff_index)
            backoff_index += 1
            stats["reconnects"] += 1
            logger.warning("Stream dropped (%s); reconnecting in %.0fs.", exc, delay)
            if should_stop():
                break
            sleep(delay)
            continue

        if done:
            break

        # The stream ended without a transport error (a server-side close): come
        # back after the shortest backoff, resuming from the last committed id.
        stats["reconnects"] += 1
        logger.info("Stream ended; reconnecting in %.0fs.", backoffs[0])
        sleep(backoffs[0])

    drain()
    logger.info(
        "Run finished: %d events, %d rows, %d flushes, %d reconnects.",
        stats["events"],
        stats["rows"],
        stats["flushes"],
        stats["reconnects"],
    )
    return stats


def _install_signal_handlers() -> dict[str, bool]:
    """Turn SIGINT/SIGTERM into a stop flag so the buffer flushes before exit."""
    state = {"stop": False}

    def handler(signum, _frame):
        logger.info("Received signal %s; finishing the current batch.", signum)
        state["stop"] = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after this many events (default: run until interrupted).",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=DEFAULT_FLUSH_EVERY,
        help=f"Rows buffered before an INSERT (default: {DEFAULT_FLUSH_EVERY}).",
    )
    args = parser.parse_args()

    configure_logging()
    validate_env()
    state = _install_signal_handlers()

    table = full_table(TABLE_NAME)
    connection, cursor = connect()
    try:
        ensure_table(cursor, table, RAW_COLUMNS)
        ensure_columns(cursor, table, RAW_COLUMNS)
        logger.info("Streaming %s into %s.", STREAM_URL, table)
        consume(
            cursor,
            max_events=args.max_events,
            flush_every=args.flush_every,
            should_stop=lambda: state["stop"],
        )
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
