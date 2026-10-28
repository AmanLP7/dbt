"""Configuration and SQLite schema shared by the Wikimedia Kafka flow.

The flow is three processes: a producer that bridges Wikimedia's HTTP firehose
into a Kafka topic, a broker, and a consumer that writes the topic into SQLite.
This module holds what the producer and the consumer must agree on: the stream
URL, the topic name, the database path, and the tables.

SQLite has no schemas in the Databricks sense. The database is one file
(`data/wikimedia.db`) and the sink lives in its default schema:

    main.events          one row per Wikimedia event, keyed by `meta.id`
    main.kafka_offsets   next offset per topic/partition, written in the same
                         transaction as the rows it covers

Nothing here imports `confluent_kafka`, so the SQLite pieces and their tests run
without the broker client installed.
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

# This module sits two directories inside the flow: ingestion/wikimedia/.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "wikimedia.db"

# Wikimedia EventStreams. NDJSON (`Accept: application/json`) instead of SSE: one
# JSON object per line, no `event:`/`id:` framing to parse. The stream is public
# and needs no credential, but Wikimedia throttles anonymous clients.
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "dbt_wikimedia_pipeline/1.0 (https://github.com/amanlp7/dbt-learn)"

# One topic, one partition: the sink is append-only and total arrival order is
# worth more than parallelism at ~30 events/s.
TOPIC = "wikimedia.recentchange"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_PARTITIONS = 1
DEFAULT_REPLICATION_FACTOR = 1

# Micro-batching and transport bounds.
DEFAULT_FLUSH_EVERY = 500
DEFAULT_FLUSH_SECONDS = 15.0
DEFAULT_POLL_SECONDS = 1.0
DEFAULT_READ_TIMEOUT = 90.0
DEFAULT_BACKOFF_SECONDS = (1.0, 5.0, 15.0, 30.0, 60.0)
# EventStreams closes a connection after 15 minutes; reconnect deliberately
# instead of waiting for the read timeout to fire.
DEFAULT_RECONNECT_SECONDS = 14 * 60.0

# `main.events`: the raw JSON is kept verbatim in `payload`, and the columns
# beside it are the fields the stream always carries — extracted at write time so
# a plain SQL query does not need SQLite's JSON functions.
EVENT_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("event_id", "TEXT PRIMARY KEY"),
        ("ingested_at", "TEXT NOT NULL"),
        ("event_dt", "TEXT"),
        ("topic", "TEXT"),
        ("partition", "INTEGER"),
        ("offset", "INTEGER"),
        ("wiki", "TEXT"),
        ("type", "TEXT"),
        ("title", "TEXT"),
        ("user", "TEXT"),
        ("bot", "INTEGER"),
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
    # Beam worker pool (`beam_worker_pool_service`, pulled in by the table
    # environment) raises that root level back to WARNING. A level on this
    # package's own logger is what keeps the flow's INFO lines visible when
    # another library owns the root logger.
    logging.getLogger("ingestion").setLevel(level)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


def bootstrap_servers() -> str:
    """Kafka bootstrap address; `KAFKA_BOOTSTRAP_SERVERS` overrides the default."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)


def database_path() -> Path:
    """SQLite file path; `WIKIMEDIA_SQLITE_PATH` overrides the default.

    A relative override is resolved against the flow's project root, not the
    shell's cwd: the flow writes one file per pipeline, and a cwd-dependent path
    would let a second consumer silently start on an empty database.
    """
    configured = os.getenv("WIKIMEDIA_SQLITE_PATH")
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
    statement) so a single `with db:` block is one transaction — that atomicity
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

    SQLite WAL admits one writer, and this flow has two readers of the same file
    (`flink_job` and `consumer`). Starting the second one does not corrupt
    anything — every write is `INSERT OR IGNORE` and the cursor only moves from
    `heal_cursor` — but the two processes then take turns failing with "database
    is locked" at the 5 s busy timeout, which reads like a broken sink. The lock
    turns that into one clear message.

    The descriptor is kept in a module global: the lock holds exactly as long as
    the descriptor is open, which is the life of the process. An orphaned Flink
    JVM does not hold it — the JVM is not this process — so a crashed parent
    leaves the file writable and the leftover reader keeps consuming; the
    message names the check for that case.
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
            "An orphaned Flink JVM keeps consuming after its Python parent is killed — check `pgrep -af "
            "ingestion.wikimedia`."
        ) from None
    _WRITER_LOCK = descriptor


def ensure_schema(db: sqlite3.Connection) -> None:
    """Create `main.events` and `main.kafka_offsets` if they do not exist."""
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
    # `heal_cursor` aggregates the highest stored offset per partition; without
    # this index that is a full scan of a table that only grows.
    db.execute("CREATE INDEX IF NOT EXISTS events_topic_partition_offset ON events (topic, partition, offset)")
    # `CREATE TABLE IF NOT EXISTS` cannot add a column to a database created by
    # an earlier version, and the sink is deployed against a file that already
    # holds rows. Add the column in place; its NULL means "unknown cluster".
    existing = {row[1] for row in db.execute("PRAGMA table_info(kafka_offsets)")}
    if "cluster_id" not in existing:
        db.execute("ALTER TABLE kafka_offsets ADD COLUMN cluster_id TEXT")
    db.commit()


def parse_event(payload: str | bytes) -> dict | None:
    """Decode one NDJSON event, or return None when it must not be stored.

    Rejects (None) a line that is not a JSON object, a frame with no
    `meta.id` (the dedupe key), and canary events — Wikimedia's internal
    health-check traffic, identified by `meta.domain == "canary"`, which are not
    real wiki activity. Raises nothing: a malformed line is dropped with a
    warning by the caller, never written.
    """
    if isinstance(payload, bytes):
        # `replace`, not strict: one undecodable message must not kill the sink.
        payload = payload.decode("utf-8", "replace")
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(event, dict):
        return None
    meta = event.get("meta")
    if not isinstance(meta, dict):
        return None
    if meta.get("domain") == "canary":
        return None
    # The id is the dedupe key and is encoded as a Kafka key, so a non-string
    # id (Wikimedia has never sent one) is rejected rather than crashing later.
    if not isinstance(meta.get("id"), str) or not meta["id"]:
        return None
    return event


def event_row(payload: str | bytes, ingested_at: str, partition: int, offset: int) -> tuple | None:
    """Map one Kafka message to a `main.events` row, or None to skip it.

    The columns are the fields every event carries; `payload` keeps the whole
    JSON so nothing is lost when a new field appears upstream. The `partition`
    and `offset` columns coexist with the `topic`/`partition`/`offset` inside
    `meta`, deliberately: the meta values say where Wikimedia's broker put the
    event, these say where this broker did.

    A non-UTF-8 byte is replaced rather than raised on: SQLite `TEXT` must be
    UTF-8, and one corrupt message must cost a character, not the sink.
    """
    event = parse_event(payload)
    if event is None:
        return None
    meta = event["meta"]
    return (
        str(meta["id"]),
        ingested_at,
        meta.get("dt"),
        TOPIC,
        partition,
        offset,
        event.get("wiki"),
        event.get("type"),
        event.get("title"),
        event.get("user"),
        int(bool(event.get("bot"))),
        payload.decode("utf-8", "replace") if isinstance(payload, bytes) else payload,
    )


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


def write_batch(
    db: sqlite3.Connection,
    rows: list[tuple],
    checkpoints: dict[tuple[str, int], int],
    cluster_id: str | None = None,
) -> int:
    """Insert events and advance offsets in one transaction.

    `rows` are positional tuples matching `EVENT_COLUMNS`; duplicates are
    ignored by primary key so an at-least-once replay costs nothing. `checkpoints`
    maps `(topic, partition)` to the next offset that covers the batch. The
    offset update is in the same transaction as the inserts, so a crash either
    keeps both or neither. `cluster_id` is stored beside it so a checkpoint from
    a different broker is recognisable on restart.
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
            "INSERT INTO kafka_offsets (topic, partition, next_offset, cluster_id, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(topic, partition) DO UPDATE SET "
            "next_offset = excluded.next_offset, cluster_id = excluded.cluster_id, "
            "updated_at = excluded.updated_at",
            [
                (topic, partition, next_offset, cluster_id, utc_now())
                for (topic, partition), next_offset in checkpoints.items()
            ],
        )
    return db.total_changes - before


def heal_cursor(db: sqlite3.Connection, topic: str) -> int:
    """Advance `main.kafka_offsets` to the rows `main.events` already holds.

    The Python consumer writes the cursor in the same transaction as the rows it
    covers, so its cursor is always exact. The Flink job cannot: its sink emits
    one INSERT per batch, and an offset update has no way into that transaction.
    For that reader the cursor is recovered from the rows instead - the highest
    stored `offset` per partition, plus one.

    Recovery only ever moves the cursor forward - the `DO UPDATE` is gated on
    `excluded.next_offset > kafka_offsets.next_offset` - so it can never point
    past a committed event, and a partition already at or beyond the recovered
    value is left untouched. Gating on the comparison, rather than writing
    unconditionally, is what makes the return value meaningful: a start where
    nothing needs recovering reports zero rows and logs nothing.

    A partition whose last rows were all skipped (canary, no `meta.id`) has no
    row to recover from and keeps whatever the last exact write said; the next
    message on that partition replays a few rows, which `INSERT OR IGNORE`
    absorbs.
    """
    before = db.total_changes
    with db:
        db.execute(
            "INSERT INTO kafka_offsets (topic, partition, next_offset, cluster_id, updated_at) "
            "SELECT topic, partition, MAX(offset) + 1, NULL, ? FROM events WHERE topic = ? "
            "GROUP BY topic, partition "
            "ON CONFLICT(topic, partition) DO UPDATE SET "
            "next_offset = excluded.next_offset, "
            "updated_at = excluded.updated_at "
            "WHERE excluded.next_offset > kafka_offsets.next_offset",
            (utc_now(), topic),
        )
    return db.total_changes - before
