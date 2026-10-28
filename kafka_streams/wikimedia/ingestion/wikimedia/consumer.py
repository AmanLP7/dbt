"""Consume `wikimedia.recentchange` and write it into SQLite.

    python -m ingestion.wikimedia.consumer
    python -m ingestion.wikimedia.consumer --max-events 1000
    python -m ingestion.wikimedia.consumer --max-seconds 30

The source of truth for progress is SQLite, and only SQLite. The consumer never
calls `commit()` and auto-commit is off, so there is no Kafka-side cursor and no
fallback if the database is lost; on start it reads `main.kafka_offsets` and
assigns the partition at that offset. One transaction covers the event inserts
and the offset update, so the sink can lag the broker but never point past rows
it does not have.

`INSERT OR IGNORE` on `event_id` (`meta.id`) makes the producer's at-least-once
delivery harmless: a replay writes nothing and still advances the offset.

WAL allows concurrent readers but exactly one writer. Run one consumer per
database file; two will interleave their offset updates.

`flink_job.py` is the alternative reader for the same topic and the same tables.
Where the two must agree - the resume offset, the retention gap, the cluster id
- the code is in `_kafka.py` and both import it.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from typing import Any, Protocol

from confluent_kafka import KafkaError, Message

from ingestion.wikimedia._common import (
    DEFAULT_FLUSH_EVERY,
    DEFAULT_FLUSH_SECONDS,
    DEFAULT_POLL_SECONDS,
    TOPIC,
    acquire_writer_lock,
    bootstrap_servers,
    configure_logging,
    connect_db,
    database_path,
    ensure_schema,
    event_row,
    logger,
    utc_now,
    write_batch,
)
from ingestion.wikimedia._kafka import assign_partitions, build_consumer, start_offset

__all__ = [
    "Poller",
    "assign_partitions",
    "build_consumer",
    "consume",
    "event_row",
    "main",
    "start_offset",
]


class Poller(Protocol):
    """The one method `consume` needs, so tests can substitute a fake."""

    def poll(self, timeout: float) -> Any: ...


def consume(
    db,
    consumer: Poller,
    *,
    max_events: int | None = None,
    max_seconds: float | None = None,
    flush_every: int = DEFAULT_FLUSH_EVERY,
    flush_seconds: float = DEFAULT_FLUSH_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    should_stop: Callable[[], bool] = lambda: False,
    clock: Callable[[], float] = time.monotonic,
    cluster_id: str | None = None,
) -> dict[str, int]:
    """Poll, buffer, and write batches until a bound is hit or stopped."""
    stats = {"messages": 0, "rows": 0, "skipped": 0, "flushes": 0, "errors": 0}
    rows: list[tuple] = []
    checkpoints: dict[tuple[str, int], int] = {}
    deadline = clock() + flush_seconds
    started = clock()

    def flush() -> None:
        nonlocal rows, checkpoints, deadline
        if rows or checkpoints:
            changed = write_batch(db, rows, checkpoints, cluster_id)
            # Every changed row is an insert or an offset update; subtract the
            # number of checkpoint upserts to count inserted events.
            stats["rows"] += changed - len(checkpoints)
            stats["flushes"] += 1
            rows = []
            checkpoints = {}
            logger.debug("Flushed batch (%d changes).", changed)
        deadline = clock() + flush_seconds

    while not should_stop():
        if max_seconds is not None and clock() - started >= max_seconds:
            break
        message: Message | None = consumer.poll(poll_seconds)
        if message is None:
            if rows and clock() >= deadline:
                flush()
            continue
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            stats["errors"] += 1
            logger.error("Consumer error: %s", error)
            continue

        # librdkafka's stubs mark value/partition/offset Optional; a message
        # returned without an error always carries all three.
        value = message.value()
        partition = message.partition()
        offset = message.offset()
        if value is None or partition is None or offset is None:
            stats["errors"] += 1
            logger.error("Message without value/partition/offset; skipping.")
            continue

        stats["messages"] += 1
        # Only TOPIC is assigned, so the message's topic is TOPIC; the fallback
        # is for librdkafka's Optional return, not for multi-topic consumption.
        checkpoints[(message.topic() or TOPIC, partition)] = offset + 1
        row = event_row(value, utc_now(), partition, offset)
        if row is None:
            stats["skipped"] += 1
        else:
            rows.append(row)

        if len(rows) >= flush_every or clock() >= deadline:
            flush()
        if max_events is not None and stats["messages"] >= max_events:
            break

    flush()
    logger.info(
        "Run finished: %d messages, %d rows, %d skipped, %d flushes, %d errors.",
        stats["messages"],
        stats["rows"],
        stats["skipped"],
        stats["flushes"],
        stats["errors"],
    )
    return stats


def _install_signal_handlers() -> dict[str, bool]:
    """Turn SIGINT/SIGTERM into a stop flag so the last buffer is written."""
    import signal

    state = {"stop": False}

    def handler(signum, _frame):
        logger.info("Received signal %s; writing the current batch.", signum)
        state["stop"] = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--max-events", type=int, default=None, help="Stop after this many messages.")
    parser.add_argument("--max-seconds", type=float, default=None, help="Stop after this many seconds.")
    parser.add_argument("--flush-every", type=int, default=DEFAULT_FLUSH_EVERY, help="Rows per SQLite transaction.")
    parser.add_argument("--db", default=None, help="SQLite path (default: data/wikimedia.db).")
    parser.add_argument(
        "--allow-gap",
        action="store_true",
        help="Start anyway when retention deleted the stored offset, accepting the missing range.",
    )
    args = parser.parse_args()

    configure_logging()
    db_path = args.db or database_path()
    acquire_writer_lock(db_path)
    db = connect_db(db_path)
    ensure_schema(db)
    logger.info("Sink: %s (main.events, main.kafka_offsets).", db_path)

    consumer = build_consumer(bootstrap_servers())
    try:
        _, cluster_id = assign_partitions(db, consumer, TOPIC, allow_gap=args.allow_gap)
        state = _install_signal_handlers()
        consume(
            db,
            consumer,
            max_events=args.max_events,
            max_seconds=args.max_seconds,
            flush_every=args.flush_every,
            should_stop=lambda: state["stop"],
            cluster_id=cluster_id,
        )
    finally:
        db.close()
        consumer.close()


if __name__ == "__main__":
    main()
