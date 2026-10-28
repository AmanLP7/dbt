"""Kafka positioning for the one reader of `bluesky.jetstream`.

The Flink job (`flink_job.py`) must answer one question before it reads a byte:
*where does this topic resume?* The answer comes from SQLite
(`main.kafka_offsets`), is clamped by the broker's watermarks, and is refused
outright when retention has deleted the stored position.

`flink_job.py` calls `plan_start_offsets` and passes the result to the Kafka
source's `'scan.startup.specific-offsets'` option; the job then fetches for
itself. The `confluent_kafka` client opened here is metadata-only and is closed
before the job is submitted.

`_common` deliberately has no `confluent_kafka` import (its SQLite pieces are
tested without the client installed), which is why this is a separate module.
"""

from __future__ import annotations

from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaException, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient

from ingestion.bluesky._common import (
    TOPIC,
    logger,
    stored_cluster_id,
    stored_offset,
    stored_topic_id,
)


class Cluster(Protocol):
    """The `Consumer` methods `resolve_offsets` needs, so tests can fake them."""

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any: ...

    def get_watermark_offsets(self, partition: TopicPartition, timeout: float = -1) -> tuple[int, int]: ...


def build_metadata_client(bootstrap: str, group_id: str = "bluesky-flink-plan") -> Consumer:
    """A metadata-only client: it reads watermarks and ids, never messages.

    Nothing polls this client, so every option that governs fetching or
    committing is left out; `group.id` exists only because librdkafka requires
    one. Flink owns the actual read and its own consumer group.
    """
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
            "group.id": group_id,
        }
    )


def start_offset(stored: int | None, low: int, high: int) -> int:
    """Where to start reading, given SQLite's cursor and the log's watermarks.

    - No stored offset: the earliest retained message (`low`), so a first run
      reads the backlog rather than skipping it.
    - Stored offset inside the log: exactly there.
    - Stored offset below `low`: retention deleted it, so the only reachable
      position is `low`. This is a gap in the sink, not a replay, and
      `resolve_offsets` refuses to take it unless told to.
    - Stored offset above `high`: the topic was recreated or the volume reset, so
      the recorded position is meaningless; start at `low` again.
    """
    if stored is None or stored > high:
        return low
    return max(stored, low)


def resolve_offsets(
    db,
    cluster: Cluster,
    topic: str = TOPIC,
    *,
    allow_gap: bool = False,
    topic_id: str | None = None,
) -> tuple[dict[tuple[str, int], int], str | None]:
    """Where every partition of `topic` should start, and the broker's cluster id.

    The cluster id is returned with the offsets because an offset only means
    something against the log that produced it, and the caller stores it beside
    every checkpoint.

    `topic_id` is the same idea one level down. A topic deleted and recreated on
    the same broker keeps the cluster id and restarts offsets at 0, so a stored
    offset lands *inside* the new log and reading from it silently drops the
    first `stored` messages. The topic id changes when the log does, so a
    mismatch resets the partition to `low` exactly as a foreign cluster does.
    Leaving it None means "unknown" and skips that comparison.
    """
    metadata = cluster.list_topics(topic, timeout=10)
    if topic not in metadata.topics:
        raise SystemExit(f"Topic {topic!r} does not exist; run `python -m ingestion.bluesky.producer --create-topic`.")
    cluster_id = metadata.cluster_id or None
    offsets: dict[tuple[str, int], int] = {}
    for partition in sorted(metadata.topics[topic].partitions):
        low, high = cluster.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        stored = stored_offset(db, topic, partition)
        recorded_cluster = stored_cluster_id(db, topic, partition)
        recorded_topic = stored_topic_id(db, topic, partition)
        foreign_cluster = (
            stored is not None
            and recorded_cluster is not None
            and cluster_id is not None
            and recorded_cluster != cluster_id
        )
        foreign_topic = (
            stored is not None and recorded_topic is not None and topic_id is not None and recorded_topic != topic_id
        )
        if foreign_cluster or foreign_topic:
            # A fresh log reuses offset numbers from 0, so the checkpoint can
            # land inside it and silently skip its first N messages. The cluster
            # id catches a recreated broker; the topic id catches a topic that
            # was deleted and recreated underneath it.
            offset = low
            logger.error(
                "%s[%d] checkpoint belongs to %s, but this is %s; restarting at %d.",
                topic,
                partition,
                f"Kafka cluster {recorded_cluster}" if foreign_cluster else f"topic id {recorded_topic}",
                f"cluster {cluster_id}" if foreign_cluster else f"topic id {topic_id}",
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
                        "them silently, so the job stopped. Re-run with --allow-gap to accept the hole, or "
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
        offsets[(topic, partition)] = offset
    return offsets, cluster_id


def current_identity(bootstrap: str, topic: str = TOPIC) -> tuple[str | None, str | None]:
    """`(cluster_id, topic_id)` for the log the caller is about to read.

    Both ids answer "is the log I am about to resume the one my checkpoint came
    from?", and they catch different recreations: a deleted broker changes the
    cluster id but not the topic id, and a topic deleted and recreated on the
    same broker changes the topic id and leaves the cluster id alone. Only the
    pair covers both, and only the topic id covers the case where a checkpoint
    inside the new log's range is silently plausible.

    The Flink job needs both *before* `plan_start_offsets` runs: `heal_cursor`
    recovers a position from `main.events`, and those rows are only this log's
    if the ids say so. One admin client, two metadata round trips. There is no
    `close()` to call -- `AdminClient` has none -- so the handle is released
    when the reference drops on return; a lifetime shorter than that is not
    available. The topic id is not on `Consumer.list_topics()`'s
    `TopicMetadata`, which is why this is an `AdminClient` rather than the
    metadata client the planner already opens.

    An unknown topic yields `topic_id=None` and an error line: `resolve_offsets`
    turns that into the actionable "create the topic" exit.

    The broker hands the id over as a `cimpl.Uuid`, not a string: it prints as
    the base64 id and compares equal to nothing, not even `str()` of itself, so
    it is normalised here. Left as a `Uuid` it would raise on the SQLite bind and
    never match the `TEXT` column it is stored in.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    cluster_id = admin.list_topics(timeout=10).cluster_id or None
    try:
        described = admin.describe_topics(TopicCollection([topic]), request_timeout=10)
        reported = described[topic].result().topic_id
        topic_id = str(reported) if reported is not None else None
    except KafkaException as error:
        logger.error("Could not read the topic id for %s: %s", topic, error)
        return cluster_id, None
    return cluster_id, topic_id


def plan_start_offsets(
    db,
    bootstrap: str,
    topic: str = TOPIC,
    *,
    allow_gap: bool = False,
    topic_id: str | None = None,
) -> tuple[dict[tuple[str, int], int], str | None]:
    """Resolve the resume position for every partition, then close the client.

    Returns `{(topic, partition): offset}` and the broker's cluster id. The
    `confluent_kafka.Consumer` opened here is metadata-only: Flink assigns and
    fetches for itself, so this one is closed before the job is submitted. It
    exists because librdkafka is the only thing in the environment that can ask
    a broker for a cluster id and a partition's watermarks.

    `topic_id` comes from `current_identity` and is what lets the planner notice
    a recreated topic; without it only the cluster id is compared.
    """
    client = build_metadata_client(bootstrap, group_id="bluesky-flink-plan")
    try:
        return resolve_offsets(db, client, topic, allow_gap=allow_gap, topic_id=topic_id)
    finally:
        client.close()
