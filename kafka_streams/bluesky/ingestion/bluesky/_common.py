"""Configuration and SQLite schema shared by the Bluesky Kafka flow.

The flow is three processes: a producer that bridges Bluesky's Jetstream
WebSocket into a Kafka topic, a broker, and a reader that writes the topic into
SQLite. This module holds what the producer and the readers must agree on: the
upstream URL, the topic name, the database path, and the tables.

SQLite has no schemas in the Databricks sense. The database is one file
(`data/bluesky.db`) and the sink lives in its default schema:

    main.events            one row per Jetstream event, keyed by its `seq`
    main.kafka_offsets     next offset per topic/partition, written in the same
                           transaction as the rows it covers
    main.jetstream_cursor  the upstream position to resume from, written by the
                           producer only -- it is the one store of upstream
                           position this flow has

Nothing here imports `confluent_kafka` or `websockets`, so the SQLite pieces and
their tests run without either client installed.
"""

from __future__ import annotations

import datetime
import fcntl
import json
import logging
import os
import sqlite3
from collections import OrderedDict
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# This module sits two directories inside the flow: ingestion/bluesky/.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "bluesky.db"

# Bluesky Jetstream, v2. Public, unauthenticated, JSON over WebSocket: one
# object per frame, already decoded from the network's DAG-CBOR, so no CAR
# reader is needed. v2 is the endpoint Bluesky recommends for new projects and
# the only one with replay and snapshotting; v1 (`jetstream2.us-east...`) is
# legacy and its frames are 110 B smaller for the same event.
#
# The subprotocol is mandatory: the server rejects the upgrade without it.
JETSTREAM_URL = "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
SUBPROTOCOL = "xrpc.v1.json"
USER_AGENT = "dbt_bluesky_pipeline/1.0 (https://github.com/amanlp7/dbt-learn)"

# One topic, one partition: the sink is append-only and total arrival order is
# worth more than parallelism at ~300 events/s.
TOPIC = "bluesky.jetstream"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_PARTITIONS = 1
DEFAULT_REPLICATION_FACTOR = 1

# Record types worth keeping. The firehose is 68% likes and 11% reposts; storing
# all of it costs 16 GiB/day raw to answer questions about posts. An empty tuple
# means "everything", which is what the flow ships with -- narrowing it is a
# filter change, not a code change.
DEFAULT_COLLECTIONS: tuple[str, ...] = ()
DEFAULT_KINDS: tuple[str, ...] = ("commit", "account", "identity")

# Transport bounds.
DEFAULT_READ_TIMEOUT = 90.0
DEFAULT_BACKOFF_SECONDS = (1.0, 5.0, 15.0, 30.0, 60.0)
# Jetstream closes a connection when the client stops answering pings; the
# measured timeout was 39 s on a silent probe. Reconnect deliberately rather
# than waiting for a read timeout is not needed here -- the socket raises -- so
# this is only the upper bound on how long one connection is kept.
DEFAULT_RECONNECT_SECONDS = 30 * 60.0
# Ping cadence and how long a missing pong is tolerated. The default 20 s
# timeout killed a probe mid-stream; keepalive traffic is tiny and the cost of
# a false disconnect is a reconnect, so be generous.
DEFAULT_PING_INTERVAL = 20.0
DEFAULT_PING_TIMEOUT = 60.0
# Jetstream's replay window, measured from this machine on 2026-10-27: a cursor
# 25 h back replays exactly (90,001 s served for a 90,000 s request), 72 h back
# is silently clamped to the oldest retained event (served age 132,742 s =
# 36.87 h). Past that the server answers with a position it chooses, so the
# producer checks the first replayed event's age against the cursor it asked for
# and logs loudly on a clamp.
#
# `RETAINED_WINDOW_SECONDS` is the threshold below which a clamp is impossible,
# and the clamp test only runs above it. Without that gate any forward jump of
# more than `CLAMP_TOLERANCE_SECONDS` reads as a clamp -- including the ordinary
# quiet stretch of a narrow `--collections` filter, which is a gap in the events
# that *match*, not in the ones the server retained.
RETAINED_WINDOW_SECONDS = 36 * 3600.0
CLAMP_TOLERANCE_SECONDS = 600.0

# `main.events`: the raw frame is kept verbatim in `payload`, and the columns
# beside it are the fields every Jetstream event carries -- extracted at write
# time so a plain SQL query does not need SQLite's JSON functions.
#
# `seq` is the primary key: Jetstream assigns it per event, globally and
# monotonically, so it identifies the event and nothing else. It is not this
# broker's `offset`.
#
# The dedupe key is deliberately **not** the `at://` record URI. Measured on the
# first 3,500 messages (2026-10-27): 15 events -- 0.43% -- were a second event
# for a record already in the table (an `update` or a `delete` after a `create`),
# and keying on the URI silently discarded them. A record URI identifies the
# record, not the event, so it cannot be the primary key of an event log. A
# replay still costs nothing: the replayed event carries the same `seq`.
#
# `record_key` is kept as a plain column -- not a Jetstream field, but
# `at://<did>/<collection>/<rkey>` for a commit and `at://<did>/<kind>/<seq>` for
# an account or identity event -- because "everything that happened to one
# record" is the query the stream is actually read for.
EVENT_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("record_key", "TEXT NOT NULL"),
        ("ingested_at", "TEXT NOT NULL"),
        ("event_dt", "TEXT"),
        ("event_kind", "TEXT"),
        ("collection", "TEXT"),
        ("did", "TEXT"),
        ("operation", "TEXT"),
        ("rkey", "TEXT"),
        ("seq", "INTEGER PRIMARY KEY"),
        ("topic", "TEXT"),
        ("partition", "INTEGER"),
        ("offset", "INTEGER"),
        ("payload", "TEXT NOT NULL"),
    ]
)

OFFSET_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("topic", "TEXT NOT NULL"),
        ("partition", "INTEGER NOT NULL"),
        ("next_offset", "INTEGER NOT NULL"),
        # Kafka cluster id at the time of writing. A checkpoint is only
        # meaningful against the cluster it came from, so a different id means
        # the broker was deleted and recreated and the offset must be reset.
        ("cluster_id", "TEXT"),
        # Kafka *topic* id at the time of writing. A topic deleted and recreated
        # on the same broker keeps the cluster id, so only this distinguishes
        # "resumed" from "the log restarted and already holds `next_offset`
        # messages" -- the one case where a plausible checkpoint skips a backlog.
        ("topic_id", "TEXT"),
        ("updated_at", "TEXT NOT NULL"),
    ]
)

# One row: the upstream position to resume the WebSocket from, as epoch
# microseconds -- the same unit as `payload.time`, not a `seq`. Keyed by stream
# name so a second Jetstream subscription could be added without a migration.
CURSOR_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("stream", "TEXT PRIMARY KEY"),
        ("next_cursor", "INTEGER NOT NULL"),
        ("updated_at", "TEXT NOT NULL"),
    ]
)

PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA busy_timeout = 5000",
)

logger = logging.getLogger(__name__)

# Descriptor of the exclusive lock on the SQLite file; see `acquire_writer_lock`.
_WRITER_LOCK: int | None = None


def configure_logging() -> None:
    """Configure root logging once per process; `LOG_LEVEL` overrides INFO."""
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # `logging.basicConfig` only sets the root level, and importing PyFlink's
    # Beam worker pool raises that root level back to WARNING. A level on this
    # package's own logger is what keeps the flow's INFO lines visible when
    # another library owns the root logger.
    logging.getLogger("ingestion").setLevel(level)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def bootstrap_servers() -> str:
    """Kafka bootstrap address; `KAFKA_BOOTSTRAP_SERVERS` overrides the default."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)


def jetstream_url() -> str:
    """Upstream WebSocket URL; `BLUESKY_JETSTREAM_URL` overrides the default.

    The override exists for the self-hosted Jetstream binary, which serves the
    same protocol from a local port.
    """
    return os.getenv("BLUESKY_JETSTREAM_URL", JETSTREAM_URL)


def database_path() -> Path:
    """SQLite file path; `BLUESKY_SQLITE_PATH` overrides the default.

    A relative override is resolved against the flow's project root, not the
    shell's cwd: a cwd-dependent path would let a second reader silently start
    on an empty database.
    """
    configured = os.getenv("BLUESKY_SQLITE_PATH")
    if not configured:
        return DEFAULT_DB_PATH
    path = Path(configured)
    return path if path.is_absolute() else PROJECT_ROOT / path


def utc_now() -> str:
    """Ingest timestamp as an ISO-8601 string with a UTC offset."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def connect_db(path: Path | str | None = None) -> sqlite3.Connection:
    """Open the SQLite database with the pragmas the sink depends on.

    `isolation_level` is left at the default (implicit `BEGIN` before a DML
    statement) so a single `with db:` block is one transaction -- that atomicity
    is what keeps `main.kafka_offsets` from running ahead of `main.events`.

    `path` accepts a `str` because `--db` arrives that way; normalising here
    keeps every caller from having to remember it.
    """
    db_path = Path(path) if path else database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    for pragma in PRAGMAS:
        connection.execute(pragma)
    return connection


def acquire_writer_lock(path: Path | str | None = None) -> None:
    """Take an exclusive lock on the database file, or stop before writing to it.

    SQLite WAL admits one writer, and this flow has two processes on the same
    file (the producer's cursor and `flink_job`). Starting two writers
    does not corrupt anything -- every write is `INSERT OR IGNORE` -- but the
    two processes then take turns failing with "database is locked" at the 5 s
    busy timeout, which reads like a broken sink. The lock turns that into one
    clear message.

    The descriptor is kept in a module global: the lock holds exactly as long as
    the descriptor is open, which is the life of the process. A Flink JVM is a
    child of this process and is not covered by the descriptor, but it does not
    outlive its parent: PyFlink installs a Watchdog, and after `SIGKILL` of the
    Python parent the JVM was gone within 3 s (measured 2026-10-28, with 6 rows
    from its last sink batch landing in between). The window is real but
    seconds long and SQLite serialises anyway, so the message names the check
    for an operator who finds a JVM still running.
    """
    global _WRITER_LOCK
    db_path = Path(path) if path else database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(db_path.with_suffix(db_path.suffix + ".lock"), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(descriptor)
        raise SystemExit(
            f"{db_path} is already being written by another process. This flow allows one writer per "
            "database file: stop the running reader (or point --db elsewhere) before starting a second one. "
            "An orphaned Flink JVM keeps consuming after its Python parent is killed -- check `pgrep -af "
            "ingestion.bluesky`."
        ) from None
    _WRITER_LOCK = descriptor


def ensure_schema(db: sqlite3.Connection) -> None:
    """Create the three tables and their indexes if they do not exist."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS events ("
        + ", ".join(f"{name} {sql_type}" for name, sql_type in EVENT_COLUMNS.items())
        + ")"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS kafka_offsets ("
        + ", ".join(f"{name} {sql_type}" for name, sql_type in OFFSET_COLUMNS.items())
        + ", PRIMARY KEY (topic, partition))"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS jetstream_cursor ("
        + ", ".join(f"{name} {sql_type}" for name, sql_type in CURSOR_COLUMNS.items())
        + ")"
    )
    # `CREATE TABLE IF NOT EXISTS` does not widen a table that predates a
    # column, and every deployed sink predates `topic_id`. Old checkpoints keep
    # NULL there, which the comparisons read as "unknown log" and the next write
    # fills in; refusing to start would strand a working sink.
    if "topic_id" not in {row[1] for row in db.execute("PRAGMA table_info(kafka_offsets)")}:
        db.execute("ALTER TABLE kafka_offsets ADD COLUMN topic_id TEXT")
    # `heal_cursor` aggregates the highest stored offset per partition; without
    # this index that is a full scan of a table that only grows.
    db.execute("CREATE INDEX IF NOT EXISTS events_topic_partition_offset ON events (topic, partition, offset)")
    # `seq` is the primary key, so SQLite already indexes it. This one serves the
    # query the stream is read for -- every event for one record, in order.
    db.execute("CREATE INDEX IF NOT EXISTS events_record_key ON events (record_key, seq)")
    db.commit()


def event_kind(payload: dict) -> str | None:
    """The trailing component of the payload's `$type`: `commit`, `account`, ..."""
    declared = payload.get("$type")
    if not isinstance(declared, str) or "#" not in declared:
        return None
    return declared.rsplit("#", 1)[1] or None


def record_key(payload: dict) -> str | None:
    """The record an event touches, as an `at://` URI, or None when it names nothing.

    This is a lookup key, not the dedupe key -- `main.events` is keyed on `seq`,
    because one record can have several events (create, then update or delete)
    and the URI cannot tell them apart.

    A commit names a record, so its URI is `<did>/<collection>/<rkey>`. An
    account or identity event names an account, not a record, and the only
    monotonic thing it carries is its own `seq` -- so that is what the key uses.
    """
    did = payload.get("did")
    if not isinstance(did, str) or not did:
        return None
    collection = payload.get("collection")
    rkey = payload.get("rkey")
    if isinstance(collection, str) and isinstance(rkey, str) and collection and rkey:
        return f"at://{did}/{collection}/{rkey}"
    kind = event_kind(payload)
    seq = payload.get("seq")
    if kind and isinstance(seq, int):
        return f"at://{did}/{kind}/{seq}"
    return None


def parse_event(frame: str | bytes) -> dict | None:
    """Decode one WebSocket frame into a Jetstream payload, or None to drop it.

    Three rejection classes, none fatal:

    - a frame that is not a JSON object (malformed, or a partial write);
    - an envelope whose `$type` is not `message` -- v2 wraps every event as
      `{"$type": "message", "payload": {...}}`, and an error frame carries
      `$type: "error"` with the reason inside;
    - a payload with no `seq`. `seq` is the resume cursor and the row's
      identity for non-commit events, so an event without one cannot be
      replayed idempotently and is dropped rather than stored twice.

    A v1 frame is accepted too: it is the same payload without the envelope,
    which keeps the parser usable against a v1 endpoint without a second code
    path.
    """
    if isinstance(frame, bytes):
        # `replace`, not strict: one undecodable frame must not kill the sink.
        frame = frame.decode("utf-8", "replace")
    try:
        decoded = json.loads(frame)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict):
        return None
    if "payload" in decoded:
        if decoded.get("$type") != "message":
            return None
        payload = decoded["payload"]
    else:
        payload = decoded
    if not isinstance(payload, dict):
        return None
    if not isinstance(payload.get("seq"), int):
        return None
    if record_key(payload) is None:
        return None
    return payload


def frame_error(frame: str | bytes) -> str | None:
    """The reason carried by a v2 `$type: error` frame, or None for any other frame.

    v2 answers a bad cursor or a throttled client with an error envelope rather
    than by closing, and `parse_event` drops it because it carries no payload.
    Without this the reason never reaches the log: a rejected cursor would look
    exactly like a corrupt frame in the producer's `dropped` count.

    Only called for frames `parse_event` already rejected, so a good frame is
    decoded once.
    """
    if isinstance(frame, bytes):
        frame = frame.decode("utf-8", "replace")
    try:
        decoded = json.loads(frame)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict) or decoded.get("$type") != "error":
        return None
    return str(decoded.get("message") or decoded.get("error") or decoded)[:500]


def stored_offset(db: sqlite3.Connection, topic: str, partition: int) -> int | None:
    """The next offset to consume for a partition, or None when unset."""
    row = db.execute(
        "SELECT next_offset FROM kafka_offsets WHERE topic = ? AND partition = ?",
        (topic, partition),
    ).fetchone()
    return int(row[0]) if row else None


def stored_cluster_id(db: sqlite3.Connection, topic: str, partition: int) -> str | None:
    """The cluster id recorded with a checkpoint, or None when unset."""
    row = db.execute(
        "SELECT cluster_id FROM kafka_offsets WHERE topic = ? AND partition = ?",
        (topic, partition),
    ).fetchone()
    return row[0] if row else None


def stored_topic_id(db: sqlite3.Connection, topic: str, partition: int) -> str | None:
    """The Kafka topic id recorded with a checkpoint, or None when unset.

    The discriminator the cluster id cannot be: deleting and recreating a topic
    on the same broker leaves the cluster id alone, and the new log restarts at
    offset 0 -- so a checkpoint of 120 against a refilled topic is not out of
    range, it is simply wrong, and only the topic id says so.
    """
    row = db.execute(
        "SELECT topic_id FROM kafka_offsets WHERE topic = ? AND partition = ?",
        (topic, partition),
    ).fetchone()
    return row[0] if row else None


def stored_cursor(db: sqlite3.Connection, stream: str = TOPIC) -> int | None:
    """The Jetstream position to resume from, in microseconds, or None on a first run.

    Either units work upstream, and this flow stores a timestamp rather than a
    `seq`: a `seq` gap is normal under any collection filter, so only a timestamp
    lets `_is_clamped` compare the age of the first replayed event with the age
    the cursor implies.
    """
    row = db.execute("SELECT next_cursor FROM jetstream_cursor WHERE stream = ?", (stream,)).fetchone()
    return int(row[0]) if row else None


def save_cursor(db: sqlite3.Connection, next_cursor: int, stream: str = TOPIC) -> None:
    """Record the upstream position the producer will resume from.

    Called from the producer's own connection and committed per batch. It is not
    part of `write_batch`: that transaction covers Kafka offsets, which the
    readers own, and mixing the two would tie the producer's progress to a
    reader's.
    """
    with db:
        db.execute(
            "INSERT INTO jetstream_cursor (stream, next_cursor, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(stream) DO UPDATE SET next_cursor = excluded.next_cursor, updated_at = excluded.updated_at",
            (stream, next_cursor, utc_now()),
        )


def write_batch(
    db: sqlite3.Connection,
    rows: list[tuple],
    checkpoints: dict[tuple[str, int], int],
    cluster_id: str | None = None,
    topic_id: str | None = None,
) -> int:
    """Insert events and advance offsets in one transaction.

    `rows` are positional tuples matching `EVENT_COLUMNS`; duplicates are
    ignored by primary key (`seq`) so an at-least-once replay costs nothing. `checkpoints`
    maps `(topic, partition)` to the next offset that covers the batch. The
    offset update is in the same transaction as the inserts, so a crash either
    keeps both or neither. `cluster_id` and `topic_id` are stored beside it so a
    checkpoint from a different broker, or from a topic that was deleted and
    recreated on this one, is recognisable on restart.
    """
    if not rows and not checkpoints:
        return 0
    before = db.total_changes
    with db:
        db.executemany(
            "INSERT OR IGNORE INTO events ("
            + ", ".join(EVENT_COLUMNS)
            + ") VALUES ("
            + ", ".join("?" for _ in EVENT_COLUMNS)
            + ")",
            rows,
        )
        db.executemany(
            "INSERT INTO kafka_offsets (topic, partition, next_offset, cluster_id, topic_id, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(topic, partition) DO UPDATE SET "
            "next_offset = excluded.next_offset, cluster_id = excluded.cluster_id, "
            # Never blank a known id: a caller that could not read one must not
            # disarm the checks for the next reader.
            "topic_id = COALESCE(excluded.topic_id, kafka_offsets.topic_id), "
            "updated_at = excluded.updated_at",
            [
                (topic, partition, next_offset, cluster_id, topic_id, utc_now())
                for (topic, partition), next_offset in checkpoints.items()
            ],
        )
    return db.total_changes - before


def heal_cursor(
    db: sqlite3.Connection,
    topic: str = TOPIC,
    cluster_id: str | None = None,
    topic_id: str | None = None,
) -> int:
    """Advance `main.kafka_offsets` to the rows `main.events` already holds.

    The Flink sink emits one INSERT per batch, and a Kafka offset update has no
    way into that transaction. So the cursor is recovered from the rows instead
    -- the highest stored `offset` per partition, plus one -- and only ever
    forward. Called at startup and after a clean exit; the cost of the
    approximation is a replay after a crash, never a gap.

    Two conditions keep that recovery from pointing past events that are not in
    this log, both of which the plain `MAX(offset) + 1` got wrong:

    * **Only rows ingested after the checkpoint was stamped count.** A checkpoint
      is stamped at the start of a run and healed at its end, so the rows in
      between are this run's. Aggregating every row ever stored lets a broker
      that was deleted and recreated -- whose new log restarts at offset 0 behind
      a checkpoint that is already correct at 0 -- be jumped by the *old* log's
      highest offset on the next start, skipping the whole new backlog.
    * **A checkpoint that records a different cluster or a different topic is
      left alone.** Those rows were committed against a log that no longer
      exists, and `resolve_offsets` is about to reset that partition to the
      new log's start anyway. The topic id is the half the cluster id cannot
      cover: deleting a topic on the *same* broker keeps the cluster id and
      restarts offsets at 0, which is the case where a plausible checkpoint
      silently skips a whole backlog.

    A checkpoint with a NULL `cluster_id` is healed and stamped with the cluster
    passed in: leaving NULL behind switches the recreated-broker check in
    `resolve_offsets` off for good. Passing `cluster_id=None` means "unknown"
    and skips that comparison, healing on the timestamp alone; `topic_id`
    behaves the same way.
    """
    before = db.total_changes
    with db:
        db.execute(
            "INSERT INTO kafka_offsets (topic, partition, next_offset, cluster_id, topic_id, updated_at) "
            "SELECT e.topic, e.partition, MAX(e.offset) + 1, ?, ?, ? "
            "FROM events AS e "
            "LEFT JOIN kafka_offsets AS k ON k.topic = e.topic AND k.partition = e.partition "
            "WHERE e.topic = ? "
            "AND (k.next_offset IS NULL "
            "     OR (e.ingested_at >= k.updated_at "
            "         AND (k.cluster_id IS NULL OR ? IS NULL OR k.cluster_id = ?) "
            "         AND (k.topic_id IS NULL OR ? IS NULL OR k.topic_id = ?))) "
            "GROUP BY e.topic, e.partition "
            "ON CONFLICT(topic, partition) DO UPDATE SET "
            "next_offset = excluded.next_offset, "
            "cluster_id = COALESCE(kafka_offsets.cluster_id, excluded.cluster_id), "
            "topic_id = COALESCE(kafka_offsets.topic_id, excluded.topic_id), "
            "updated_at = excluded.updated_at "
            "WHERE excluded.next_offset > kafka_offsets.next_offset "
            "AND (kafka_offsets.cluster_id IS NULL OR excluded.cluster_id IS NULL "
            "OR kafka_offsets.cluster_id = excluded.cluster_id) "
            "AND (kafka_offsets.topic_id IS NULL OR excluded.topic_id IS NULL "
            "OR kafka_offsets.topic_id = excluded.topic_id)",
            (cluster_id, topic_id, utc_now(), topic, cluster_id, cluster_id, topic_id, topic_id),
        )
    return db.total_changes - before
