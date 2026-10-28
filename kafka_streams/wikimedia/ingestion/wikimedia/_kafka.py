"""Kafka positioning shared by the two readers of `wikimedia.recentchange`.

Both readers - the `confluent_kafka` consumer (`consumer.py`) and the Flink job
(`flink_job.py`) - must answer the same question before they read a byte:
*where does this topic resume?* The answer comes from SQLite
(`main.kafka_offsets`), is clamped by the broker's watermarks, and is refused
outright when retention has deleted the stored position.

The two readers differ in how they poll; they must not differ in where they
start, so the decision lives here. `consumer.py` re-exports these names and
calls `assign_partitions`; `flink_job.py` calls `plan_start_offsets` and passes
the result to the Kafka source's `'scan.startup.specific-offsets'` option.

`_common` deliberately has no `confluent_kafka` import (its SQLite pieces are
tested without the client installed), which is why this is a separate module.
"""

from __future__ import annotations

from typing import Any, Protocol

from confluent_kafka import Consumer, TopicPartition

from ingestion.wikimedia._common import (
    logger,
    stored_cluster_id,
    stored_offset,
)


class Cluster(Protocol):
    """The `Consumer` methods `assign_partitions` needs, so tests can fake them."""

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any: ...

    def get_watermark_offsets(self, partition: TopicPartition, timeout: float = -1) -> tuple[int, int]: ...

    def assign(self, partitions: list[TopicPartition]) -> None: ...


def build_consumer(bootstrap: str, group_id: str = "wikimedia-sqlite-sink") -> Consumer:
    """A manual-commit consumer for the sink.

    Auto-commit is off: the SQLite transaction decides when a batch is done. The
    group id exists only so librdkafka has one; partitions are assigned
    explicitly, so no rebalance can move progress away from SQLite.
    """
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "enable.partition.eof": False,
        }
    )


def start_offset(stored: int | None, low: int, high: int) -> int:
    """Where to start reading, given SQLite's cursor and the log's watermarks.

    - No stored offset: the earliest retained message (`low`), so a first run
      reads the backlog rather than skipping it.
    - Stored offset inside the log: exactly there.
    - Stored offset below `low`: retention deleted it, so the only reachable
      position is `low`. This is a gap in the sink, not a replay, and
      `assign_partitions` refuses to take it unless told to.
    - Stored offset above `high`: the topic was recreated or the volume reset, so
      the recorded position is meaningless; start at `low` again.
    """
    if stored is None or stored > high:
        return low
    return max(stored, low)


def assign_partitions(
    db,
    consumer: Cluster,
    topic: str,
    *,
    allow_gap: bool = False,
) -> tuple[list[TopicPartition], str | None]:
    """Assign every partition of `topic` at the offset SQLite says to read next.

    Returns the assignment and the broker's cluster id, which the caller stores
    beside every checkpoint: an offset only means something against the cluster
    that produced it.

    The offset goes into `TopicPartition` at `assign()` time. That matters:
    `assign()` without an offset leaves the partition without a position, and a
    following `seek()` fails with `_STATE` ("Erroneous state") until a poll has
    started the fetch. Setting the offset on assignment both avoids that window
    and skips the coordinator round trip that committed-offset lookup needs.
    """
    metadata = consumer.list_topics(topic, timeout=10)
    if topic not in metadata.topics:
        raise SystemExit(
            f"Topic {topic!r} does not exist; run `python -m ingestion.wikimedia.producer --create-topic`."
        )
    cluster_id = metadata.cluster_id or None
    assignment: list[TopicPartition] = []
    for partition in sorted(metadata.topics[topic].partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        stored = stored_offset(db, topic, partition)
        recorded_cluster = stored_cluster_id(db, topic, partition)
        foreign = (
            stored is not None
            and recorded_cluster is not None
            and cluster_id is not None
            and recorded_cluster != cluster_id
        )
        if foreign:
            # A fresh cluster reuses offset numbers from 0, so the checkpoint can
            # land inside the new log and silently skip its first N messages.
            # The cluster id is what makes that detectable.
            offset = low
            logger.error(
                "%s[%d] checkpoint belongs to Kafka cluster %s, but this broker is %s; restarting at %d.",
                topic,
                partition,
                recorded_cluster,
                cluster_id,
                offset,
            )
        else:
            offset = start_offset(stored, low, high)
            if stored is None:
                logger.info(
                    "%s[%d] has no stored offset; starting at the earliest retained offset %d.",
                    topic,
                    partition,
                    offset,
                )
            elif stored < low:
                missing = low - stored
                if not allow_gap:
                    raise SystemExit(
                        f"{topic}[{partition}] stored offset {stored} is below the earliest retained offset {low}: "
                        f"{missing} messages were deleted by retention and are gone. Restarting here would skip "
                        "them silently, so the consumer stopped. Re-run with --allow-gap to accept the hole, or "
                        f"raise KAFKA_LOG_RETENTION_HOURS above the expected downtime."
                    )
                logger.error(
                    "%s[%d] --allow-gap: skipping %d messages that retention deleted (%d -> %d).",
                    topic,
                    partition,
                    missing,
                    stored,
                    low,
                )
            elif offset != stored:
                logger.warning(
                    "%s[%d] stored offset %d is outside the log [%d, %d]; restarting at %d.",
                    topic,
                    partition,
                    stored,
                    low,
                    high,
                    offset,
                )
            else:
                logger.info("Resuming %s[%d] at offset %d from SQLite.", topic, partition, offset)
        assignment.append(TopicPartition(topic, partition, offset))
    consumer.assign(assignment)
    return assignment, cluster_id


def plan_start_offsets(
    db,
    bootstrap: str,
    topic: str,
    *,
    allow_gap: bool = False,
) -> tuple[dict[tuple[str, int], int], str | None]:
    """Resolve the resume position for every partition, then close the client.

    Returns `{(topic, partition): offset}` and the broker's cluster id. The
    `confluent_kafka.Consumer` opened here is metadata-only: Flink assigns and
    fetches for itself, so this one is closed before the job is submitted. It
    exists because librdkafka is the only thing in the environment that can ask
    a broker for a cluster id and a partition's watermarks.
    """
    client = build_consumer(bootstrap, group_id="wikimedia-flink-plan")
    try:
        assignment, cluster_id = assign_partitions(db, client, topic, allow_gap=allow_gap)
    finally:
        client.close()
    return {(tp.topic, tp.partition): tp.offset for tp in assignment}, cluster_id
