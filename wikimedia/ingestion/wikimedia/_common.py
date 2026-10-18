"""Shared scaffolding for the Wikimedia EventStreams consumer.

Holds what the consumer would otherwise duplicate: the tuning constants, the
stream and table coordinates, environment validation, the Databricks client, the
idempotent target-table setup, the append-only load, and the resume checkpoint.

Importing this module loads `.env` and does nothing else: no HTTP request, no
Databricks connection.

Every value that reaches SQL goes through a `?` parameter. The only things ever
interpolated into a statement are identifiers (catalog, schema, table and column
names), which cannot be bound.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor

# The Wikimedia EventStreams firehose. Server-sent events (SSE), no auth, UTF-8
# JSON per event; `https://stream.wikimedia.org/v2/stream/` documents the other
# topics. The `id:` field of each SSE frame is a JSON array carrying the Kafka
# topic/partition/offset, which is what makes a reconnect resumable.
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

CATALOG = "workspace"
SCHEMA = "wikimedia"

# The speed layer's sink: one append-only Delta table, `payload` (the event JSON
# exactly as it arrived) plus the load timestamp. Nothing is ever updated or
# deleted — every later layer reads history and filters forward, so the stream
# and the batch replay read the same rows.
TABLE_NAME = "raw_wikimedia"

# Column name -> SQL type, in table order. `ingested_at` is last: the UTC
# timestamp the batch was loaded, not a value Wikimedia supplied (`meta.dt` is
# the event's own time and lives inside `payload`).
RAW_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("payload", "STRING"),
        ("ingested_at", "TIMESTAMP"),
    ]
)

# Micro-batch bounds: a flush happens when either is reached, so a quiet stream
# still lands rows promptly and a busy one does not build an unbounded buffer.
DEFAULT_FLUSH_EVERY = 500
DEFAULT_FLUSH_SECONDS = 15.0

# Rows per INSERT statement. One statement per flush is a single round trip, but
# an unbounded one would carry megabytes of bound parameters on a busy stream, so
# a flush is split into chunks of this many rows.
DEFAULT_INSERT_CHUNK = 500

# Reconnect backoff after a dropped or ended stream, and the per-read timeout.
DEFAULT_BACKOFF_SECONDS = (1.0, 5.0, 15.0, 30.0, 60.0)
DEFAULT_READ_TIMEOUT = 90.0

REQUIRED_ENV = ("DATABRICKS_HOSTNAME", "DATABRICKS_HTTP_PATH")

# Where the last seen SSE event id is kept, so a restart resumes instead of
# replaying the whole firehose or skipping what the process missed while down.
DEFAULT_STATE_FILE = ".state/last_event_id.json"

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def configure_logging() -> None:
    """Configure root logging once per process; `LOG_LEVEL` overrides INFO."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # The SQL connector logs every HTTP round trip at INFO; on a stream that
    # flushes continuously that buries this consumer's own lines. Keep its
    # warnings and errors.
    logging.getLogger("databricks").setLevel(logging.WARNING)


def validate_env() -> None:
    """Raise if any required variable is unset, empty, or still a `your_` placeholder."""
    bad_vars = [var for var in REQUIRED_ENV if not (value := os.getenv(var)) or "your_" in value]
    if bad_vars:
        raise RuntimeError(
            f"Missing or default environment variables found: {bad_vars}. "
            "Please configure your credentials in the .env file."
        )


def connect() -> tuple[Connection, Cursor]:
    """Open a Databricks SQL connection and its cursor.

    Credentials come from the CLI profile in `~/.databrickscfg` — the OAuth token
    saved by `databricks auth login` — not from a PAT in .env. Pass a *factory*:
    the connector calls it once to obtain a header factory, then calls that per
    request, so `credentials_provider=config.authenticate` alone fails with
    "'dict' object is not callable".
    """
    databricks_config = Config(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=lambda: databricks_config.authenticate,
    )
    return connection, connection.cursor()


def full_table(name: str) -> str:
    """Qualify a raw table name with the catalog and schema."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def load_timestamp() -> datetime.datetime:
    """Load-time timestamp stamped on every row of a batch, timezone-aware UTC."""
    return datetime.datetime.now(datetime.timezone.utc)


def state_path() -> Path:
    """Path of the resume checkpoint; `WIKIMEDIA_STATE_FILE` overrides the default."""
    return Path(os.getenv("WIKIMEDIA_STATE_FILE", DEFAULT_STATE_FILE))


def read_checkpoint(path: Path | None = None) -> str | None:
    """Return the last SSE event id written by a previous run, or None.

    A missing or unreadable checkpoint means "start from now": the stream is a
    live firehose with no beginning to replay, so there is nothing else to do.
    """
    path = path or state_path()
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        logger.warning("Ignoring unreadable checkpoint at %s.", path)
        return None
    event_id = raw.get("last_event_id")
    return str(event_id) if event_id else None


def write_checkpoint(event_id: str, path: Path | None = None) -> None:
    """Persist the last SSE event id, atomically, after the rows are committed.

    Written after the load so a crash between the two costs a replay of at most
    one batch rather than a gap: the raw table is append-only and downstream
    dedupes, so replaying is always the safe side of that trade.
    """
    path = path or state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps({"last_event_id": event_id}, indent=2) + "\n")
    tmp.replace(path)


def ensure_table(cursor: Cursor, table: str, columns: OrderedDict[str, str]) -> None:
    """Create the catalog, schema and table if they do not exist yet."""
    cursor.execute(f"CREATE CATALOG IF NOT EXISTS {CATALOG};")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA};")
    column_defs = ",\n        ".join(f"{name} {type_name}" for name, type_name in columns.items())
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n        {column_defs}\n    );")


def ensure_columns(cursor: Cursor, table: str, columns: OrderedDict[str, str]) -> None:
    """Additively migrate a pre-existing table to the given column mapping.

    Idempotent: a table that already has every column is left alone, and a table
    missing one gets one `ALTER TABLE ... ADD COLUMNS` per missing column.
    """
    cursor.execute(f"DESCRIBE TABLE {table};")
    existing = {str(row[0]).lower() for row in cursor.fetchall()}
    for name, type_name in columns.items():
        if name.lower() not in existing:
            logger.info("Adding missing column %s.%s.", table, name)
            cursor.execute(f"ALTER TABLE {table} ADD COLUMNS ({name} {type_name});")


def insert_sql(table: str, columns: OrderedDict[str, str], row_count: int = 1) -> str:
    """Build one parameterised `INSERT` carrying `row_count` rows of values.

    One statement rather than one per row because the connector's `executemany`
    issues a separate round trip per parameter set: measured against this
    warehouse, loading 50 rows took 108.7s via `executemany` and 2.2s as a single
    multi-row `INSERT`. The placeholders are all the statement itself carries; the
    values travel as bound parameters, so the SQL text stays small however wide
    the batch is.
    """
    names = ", ".join(columns)
    one_row = "(" + ", ".join("?" for _ in columns) + ")"
    values = ", ".join([one_row] * row_count)
    return f"INSERT INTO {table} ({names}) VALUES {values}"


def append_rows(
    cursor: Cursor,
    table: str,
    columns: OrderedDict[str, str],
    rows: Sequence[tuple[Any, ...]],
    chunk_size: int = DEFAULT_INSERT_CHUNK,
) -> int:
    """Append `rows` to `table`; the raw layer is insert-only.

    No delete, no merge, no dedupe: an append-only raw table means every consumer
    — the speed layer's dashboards and the batch layer's full replay — reads the
    same immutable history, and a replayed batch is harmless rather than
    destructive.

    A flush is split into `chunk_size`-row statements so a busy stream's batch
    does not become one statement carrying megabytes of bound parameters. Returns
    the number of rows written.
    """
    if not rows:
        logger.debug("No rows to append to %s; skipping.", table)
        return 0

    written = 0
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        cursor.execute(insert_sql(table, columns, len(chunk)), [value for row in chunk for value in row])
        written += len(chunk)
    return written
