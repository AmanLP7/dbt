"""Shared scaffolding for the Bluesky loaders.

Holds what every loader into this project would otherwise duplicate: the tuning
constants, environment validation, the Databricks client, the idempotent target
table setup, the SQLite reader, the batching that keeps warehouse uptime down,
and the cost model the `--dry-run` plan is priced with.

Importing this module loads `.env` and does nothing else: no SQLite read, no
Databricks connection.

Every value that reaches SQL on the warehouse goes through a `?` parameter. The
only things ever interpolated into a statement are identifiers (catalog, schema,
table and column names), which cannot be bound.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import OrderedDict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor


class SqlExecutor(Protocol):
    """The two cursor methods the batch logic uses, so a test double can stand in.

    Typing these against the connector's `Cursor` would force every test to build
    a real one; the loader only ever `execute`s and `fetchall`s.
    """

    def execute(self, operation: str, parameters: Any = None) -> Any: ...

    def fetchall(self) -> Sequence[Sequence[Any]]: ...


CATALOG = "workspace"
SCHEMA = "bluesky"
RAW_TABLE = "raw_bluesky"

# The Kafka topic the SQLite sink stores. Every read is scoped by it, so a file
# that somehow holds a second topic cannot leak rows into this table.
TOPIC = "bluesky.jetstream"

# `bluesky/ingestion/bluesky/_common.py` -> repo root, where the sibling
# `kafka_streams/` checkout keeps the database the Flink job writes.
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SQLITE_PATH = REPO_ROOT / "kafka_streams" / "bluesky" / "data" / "bluesky.db"
SQLITE_PATH_ENV = "BLUESKY_SQLITE_PATH"

PLACEHOLDER_MARKER = "your_"

REQUIRED_ENV = ("DATABRICKS_HOSTNAME", "DATABRICKS_HTTP_PATH")

# Column name -> SQL type, in `main.events` order. The target is a text mirror of
# the SQLite sink: the three counters are the only non-string columns, because
# the Flink JDBC writer stores SQLite INTEGER as BIGINT/INT and nothing else
# survives a round trip as anything but text. `ingested_at` is copied verbatim —
# it is when the event reached SQLite, not when this loader reached the
# warehouse, and restamping it would collapse a day of arrivals into one value.
RAW_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("record_key", "STRING"),
        ("ingested_at", "STRING"),
        ("event_dt", "STRING"),
        ("event_kind", "STRING"),
        ("collection", "STRING"),
        ("did", "STRING"),
        ("operation", "STRING"),
        ("rkey", "STRING"),
        ("seq", "BIGINT"),
        ("topic", "STRING"),
        ("partition", "INT"),
        ("offset", "BIGINT"),
        ("payload", "STRING"),
    ]
)

TABLE_COMMENT = (
    "Bluesky Jetstream events, landed as text: the columns of kafka_streams/bluesky main.events "
    "exactly as SQLite holds them, including the ISO-8601 timestamps. Cast in dbt."
)

# `partition` and `offset` are reserved words in Databricks SQL and `offset` is a
# keyword in SQLite too; quote on both sides rather than relying on the parser.
INSERT_COLUMNS = ", ".join(f"`{name}`" for name in RAW_COLUMNS)
SELECT_COLUMNS = ", ".join(f'"{name}"' for name in RAW_COLUMNS)

# Positional indexes of the two resume columns, derived from the mapping above so
# a reordered column cannot silently turn the watermark update into a no-op.
PARTITION_INDEX = list(RAW_COLUMNS).index("partition")
OFFSET_INDEX = list(RAW_COLUMNS).index("offset")

# DBSQL rejects any statement carrying more than this many bound parameters:
# "Parameterized query has too many parameters: 10010 parameters were given but
# the limit is 10000", measured 2026-10-28 on this warehouse with an eleven-row
# overshoot over a thirteen-column statement. It is a hard cap, not a tuning
# knob, and it — not the row count — is what bounds a batch: 769 rows is the most
# a thirteen-column INSERT can carry in one statement.
MAX_STATEMENT_PARAMETERS = 10_000

# DBSQL separately rejects a statement whose parameters total more than 1 MiB
# ("combined size of parameters is 2059161, exceeding the limit of 1048576",
# hit by the sibling job_postings loader). The 3/4 fraction leaves room for the
# connector's own framing.
PARAMETER_CHARS = 1_048_576
INSERT_CHUNK_CHARS = PARAMETER_CHARS * 3 // 4

# Rows per statement: whichever of the two caps binds first. On this data the
# character cap does — the average event is ~1,030 characters of bound text, so
# ~763 rows fit — but a payload-heavy stretch can push it lower.
MAX_ROWS_PER_STATEMENT = MAX_STATEMENT_PARAMETERS // len(RAW_COLUMNS)
INSERT_CHUNK_ROWS = MAX_ROWS_PER_STATEMENT

# --- Measured cost model -----------------------------------------------------
# Measured 2026-10-28 on warehouse 5c5d597ca7af9460 (serverless 2X-Small,
# auto_stop_mins: 1), from this loader's own INSERT statements — seventeen
# timed batches, payloads 7 KB to 767 KB of bound text.
#
# A `SELECT count(*) FROM VALUES (...)` of the same shape was also timed first
# (0.94 s / 1.69 s / 2.51 s / 3.01 s for 100 / 300 / 700 / 769 rows) and fitted
# 0.73 s + 363 K chars/s — but a real INSERT costs about twice that, because it
# binds ten thousand parameters and commits a Delta version. The INSERT numbers
# are the ones that price a run, so they are the ones below:
#
#   full batches (667-767 KB): 5.4-7.3 s, mean 6.54 s for 769 rows
#   one 7 KB batch:            2.7 s
#   least squares over all:    2.82 s fixed, 203,830 chars/s
#                              (mean residual 0.79 s; the worst point, a 10.7 s
#                              batch, is the outlier at 4.2 s)
#
# The ~198 rows/s that works out to is the pipeline's real ceiling: the
# Jetstream firehose ran at ~450 messages/s, so this is a backfill tool, not a
# streaming writer. See README.
STATEMENT_OVERHEAD_SECONDS = 2.8
PARAMETER_CHARS_PER_SECOND = 204_000

# Warehouse sizing and list price, for the `--dry-run` estimate only. A 2X-Small
# consumes 4 DBU/hour; $0.70/DBU is the AWS US list rate for serverless SQL
# (contracted rates differ — change this if yours does). The scale-to-zero
# countdown is one minute and is billed, so it is added to every run's uptime.
WAREHOUSE_SIZE = "2X-Small"
WAREHOUSE_DBU_PER_HOUR = 4.0
USD_PER_DBU = 0.70
AUTO_STOP_SECONDS = 60.0

# A batch read blocks on a writer's WAL; waiting a few seconds beats failing a
# multi-hour load because the Flink sink was flushing.
SQLITE_TIMEOUT_SECONDS = 30.0

logger = logging.getLogger(__name__)

load_dotenv()


def configure_logging() -> None:
    """Configure root logging once per process; `LOG_LEVEL` overrides INFO."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # The connector logs every retry at WARNING; the loader's own batch summary is
    # the signal that matters, and the connector's noise buries it.
    logging.getLogger("databricks").setLevel(logging.WARNING)


def require_env() -> dict[str, str]:
    """Return the connection settings, or raise naming what is unusable."""
    missing = [key for key in REQUIRED_ENV if not (value := os.environ.get(key)) or PLACEHOLDER_MARKER in value]
    if missing:
        raise RuntimeError(
            f"Missing or placeholder environment variables: {missing}. "
            "Copy .env.example to .env and fill it in, or export them."
        )
    return {key: os.environ[key] for key in REQUIRED_ENV}


def sqlite_path() -> Path:
    """The SQLite database to read: `BLUESKY_SQLITE_PATH`, else the flow's default."""
    return Path(os.environ.get(SQLITE_PATH_ENV, DEFAULT_SQLITE_PATH)).expanduser()


def databricks_config() -> Config:
    """The Databricks CLI login from `~/.databrickscfg`, which refreshes itself.

    `auth_type = databricks-cli` makes the SDK return the CLI's cached OAuth token
    and run the CLI's own refresh when it expired. That is why the loader never
    passes a token and never reads a PAT.
    """
    return Config(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))


def databricks_token() -> str:
    """A freshly minted bearer token, for callers that want a token, not a provider.

    The dbt CLI has no way to accept a credentials provider, only
    `DATABRICKS_TOKEN`, so anything that shells out to dbt mints one from this
    same config rather than trusting a value that may have been exported hours
    ago.
    """
    return databricks_config().authenticate()["Authorization"].split(" ", 1)[1]


def connect(env: dict[str, str]) -> tuple[Connection, Cursor]:
    """Open a SQL warehouse connection and its cursor.

    Credentials come from the CLI profile in `~/.databrickscfg` — the OAuth token
    saved by `databricks auth login` — not from a PAT. Pass a *factory*: the
    connector calls it once to obtain a header factory, then calls that per
    request, so `credentials_provider=config.authenticate` alone fails with
    "'dict' object is not callable".
    """
    config = databricks_config()
    connection = sql.connect(
        server_hostname=env["DATABRICKS_HOSTNAME"],
        http_path=env["DATABRICKS_HTTP_PATH"],
        credentials_provider=lambda: config.authenticate,
    )
    return connection, connection.cursor()


def raw_table_fqn() -> str:
    """Fully-qualified name of the raw table, overridable by environment."""
    catalog = os.environ.get("BLUESKY_CATALOG", CATALOG)
    schema = os.environ.get("BLUESKY_SCHEMA", SCHEMA)
    return f"{catalog}.{schema}.{RAW_TABLE}"


def ensure_raw_table(cursor: SqlExecutor, table: str | None = None) -> str:
    """Create the catalog, schema and raw table if they do not exist yet.

    Idempotent, and identical in shape to the table the flow was first landed into
    by hand (clustered on `event_dt`, which is the column every read filters on).
    Returns the fully-qualified name.
    """
    fqn = table or raw_table_fqn()
    catalog, schema, _ = fqn.split(".")
    column_defs = ",\n      ".join(f"`{name}` {type_name}" for name, type_name in RAW_COLUMNS.items())
    cursor.execute(f"CREATE CATALOG IF NOT EXISTS {catalog};")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {fqn} (
      {column_defs}
    )
    USING delta
    COMMENT '{TABLE_COMMENT}'
    CLUSTER BY (event_dt)
    TBLPROPERTIES ('delta.parquet.compression.codec' = 'zstd');"""
    )
    logger.info("Ensured raw table %s", fqn)
    return fqn


def target_watermarks(cursor: SqlExecutor, table: str | None = None) -> dict[int, int]:
    """Highest landed `offset` per partition, keyed by partition number.

    This is the resume point, and it lives in the target rather than in a local
    checkpoint so it cannot disagree with what actually landed: an empty mapping
    means nothing has been loaded yet, and a partition missing from it starts at
    its first row.
    """
    fqn = table or raw_table_fqn()
    cursor.execute(f"SELECT `partition`, MAX(`offset`) FROM {fqn} GROUP BY `partition`;")
    marks: dict[int, int] = {}
    for partition, highest in cursor.fetchall():
        if partition is None or highest is None:
            continue
        marks[int(partition)] = int(highest)
    return marks


def connect_sqlite(path: Path) -> sqlite3.Connection:
    """Open the event database read-only.

    Read-only is not politeness: the file is live, written by the Flink sink, and
    a writer connection from here would take the lock the producer also wants.
    `mode=ro` also fails loudly when the path is wrong instead of creating an
    empty database and reporting zero rows, which is the failure mode that wastes
    an afternoon.
    """
    if not path.is_file():
        raise FileNotFoundError(
            f"No SQLite database at {path}. Set {SQLITE_PATH_ENV} or pass --db; "
            "the file is written by kafka_streams/bluesky/ingestion/bluesky/flink_job.py."
        )
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=SQLITE_TIMEOUT_SECONDS)


def source_partitions(con: sqlite3.Connection) -> list[int]:
    """Partitions present in the source for `TOPIC`, ascending."""
    rows = con.execute("SELECT DISTINCT partition FROM events WHERE topic = ? ORDER BY partition", (TOPIC,))
    return [int(row[0]) for row in rows]


def source_offset_bounds(con: sqlite3.Connection) -> dict[int, tuple[int, int]]:
    """(lowest, highest) stored offset per partition — the range a load can ever see."""
    rows = con.execute(
        "SELECT partition, MIN(offset), MAX(offset) FROM events WHERE topic = ? GROUP BY partition ORDER BY partition",
        (TOPIC,),
    )
    return {int(partition): (int(low), int(high)) for partition, low, high in rows}


def log_gaps(con: sqlite3.Connection, watermarks: dict[int, int]) -> None:
    """Warn when the source no longer holds rows the target has not seen.

    Same guard the kafka_streams readers carry: retention (or a re-created SQLite
    file) can start the log above the resume point, and skipping the range
    silently is the one failure nobody notices until an analysis is wrong. The
    load continues — the rows are gone from the source, so there is nothing to
    copy — but the gap is named, with both ends.
    """
    for partition, (low, _high) in source_offset_bounds(con).items():
        landed = watermarks.get(partition)
        if landed is not None and low > landed + 1:
            logger.warning(
                "Gap on partition %d: the target has offset %d and the source starts at %d, so offsets "
                "%d-%d never reached SQLite (retention, or a re-created database). Continuing past them.",
                partition,
                landed,
                low,
                landed + 1,
                low - 1,
            )


# The per-row byte cost the server charges, mirrored in SQL for the plan: ten
# STRING columns plus three integers at 8 bytes each. DBSQL's parameter limit is
# 1 MiB of *bytes*, so this measures UTF-8 octets rather than characters — on
# this data the two differ by 0.56% (sampled 200 K rows), but a single
# emoji-heavy payload can be 2.3x its character count. `length(cast(x as blob))`
# is the byte count in every SQLite version; `octet_length()` only exists from
# 3.43.
def _byte_len(column: str) -> str:
    return f"length(cast({column} as blob))"


# `record_key` and `ingested_at` are NOT NULL, the rest are nullable, so only the
# latter need coalescing. The three counters are charged a flat 8 bytes each.
_CHAR_COST_SQL = " + ".join(
    [
        _byte_len("record_key"),
        _byte_len("ingested_at"),
        *[
            f"coalesce({_byte_len(name)}, 0)"
            for name, sql_type in RAW_COLUMNS.items()
            if sql_type == "STRING" and name not in ("record_key", "ingested_at")
        ],
        f"{sum(1 for sql_type in RAW_COLUMNS.values() if sql_type != 'STRING')} * 8",
    ]
)


def pending_range(watermarks: dict[int, int]) -> tuple[str, list[Any]]:
    """SQL predicate and parameters selecting rows the target has not seen.

    One disjunct per partition, because each keeps its own resume point. A
    partition with no watermark yet selects all of its rows.
    """
    clauses: list[str] = []
    params: list[Any] = [TOPIC]
    for partition in sorted(watermarks):
        clauses.append("(partition = ? AND offset > ?)")
        params.extend([partition, watermarks[partition]])
    if not clauses:
        return "topic = ?", params
    unbounded = "(partition NOT IN (" + ", ".join("?" for _ in watermarks) + "))"
    params.extend(sorted(watermarks))
    return "topic = ? AND (" + " OR ".join([*clauses, unbounded]) + ")", params


def pending_stats(con: sqlite3.Connection, watermarks: dict[int, int]) -> tuple[int, int]:
    """(rows, bound bytes) still to load — what the cost estimate is made of."""
    where, params = pending_range(watermarks)
    row = con.execute(f"SELECT count(*), coalesce(sum({_CHAR_COST_SQL}), 0) FROM events WHERE {where}", params)
    count, chars = row.fetchone()
    return int(count), int(chars)


def row_chars(row: Sequence[Any]) -> int:
    """Approximate the parameter cost of one row, in UTF-8 bytes.

    Strings dominate and are measured the way DBSQL's 1 MiB parameter budget
    measures them; the rest are ids and counters, which contribute a constant few
    bytes each.
    """
    return sum(len(value.encode()) if isinstance(value, str) else 8 for value in row)


def iter_batches(
    con: sqlite3.Connection,
    watermarks: dict[int, int],
    *,
    batch_rows: int = INSERT_CHUNK_ROWS,
    max_rows: int | None = None,
) -> Iterator[list[tuple[Any, ...]]]:
    """Yield the unloaded rows as batches of exactly one INSERT statement each.

    Rows are read in `(partition, offset)` order, one partition at a time, so a
    load that is interrupted leaves a contiguous watermark per partition and the
    next run resumes from it. A batch closes when adding another row would break
    either DBSQL limit; a single row larger than the byte budget is yielded alone
    rather than truncated, because the raw table's fidelity contract is worth more
    than one oversized statement.

    `max_rows` stops the scan at exactly that many rows, trimming the batch that
    crosses it, so `--limit` cannot overshoot by a whole statement's worth.
    """
    if batch_rows < 1 or batch_rows > MAX_ROWS_PER_STATEMENT:
        raise ValueError(f"batch_rows must be between 1 and {MAX_ROWS_PER_STATEMENT}, got {batch_rows}")
    for page in _iter_pages(con, watermarks, page_rows=batch_rows, max_rows=max_rows):
        yield from _split_by_bytes(page, batch_rows)


def _iter_pages(
    con: sqlite3.Connection,
    watermarks: dict[int, int],
    *,
    page_rows: int,
    max_rows: int | None = None,
) -> Iterator[list[tuple[Any, ...]]]:
    """Yield unloaded rows in `(partition, offset)` order, `page_rows` at a time.

    Each page is fetched to exhaustion with `fetchall()` *before* anything is
    yielded. That matters: the source is a live WAL database under the Flink sink,
    and a `sqlite3` cursor left open across the caller's multi-second warehouse
    statement holds a read snapshot, which stops the WAL from being checkpointed
    and grows it for as long as the load runs. Paging by `offset > last row of the
    page` costs one extra statement per page and keeps each read lock to
    milliseconds.

    Both consumers sit on top of this: `iter_batches` cuts a page into
    single-statement batches, the NDJSON exporter into multi-megabyte files. The
    paging is shared rather than copied because getting it wrong is what pins the
    WAL, and a second implementation would eventually drift from the first.
    """
    emitted = 0
    for partition in source_partitions(con):
        after = watermarks.get(partition)
        logger.debug("Reading partition %d from offset %s", partition, "-" if after is None else after + 1)

        while True:
            query = f"SELECT {SELECT_COLUMNS} FROM events WHERE topic = ? AND partition = ?"
            params: list[Any] = [TOPIC, partition]
            if after is not None:
                query += " AND offset > ?"
                params.append(after)
            query += " ORDER BY offset LIMIT ?"
            params.append(page_rows)
            # fetchall() runs the statement to completion, so no read lock
            # survives this line.
            page = [tuple(row) for row in con.execute(query, params).fetchall()]
            if not page:
                break
            after = page[-1][OFFSET_INDEX]

            if max_rows is not None:
                remaining = max_rows - emitted
                if remaining <= 0:
                    return
                page = page[:remaining]
            emitted += len(page)
            yield page
            if max_rows is not None and emitted >= max_rows:
                return


def _split_by_bytes(page: Sequence[tuple[Any, ...]], batch_rows: int) -> Iterator[list[tuple[Any, ...]]]:
    """Cut one page into as few batches as the two caps allow."""
    rows: list[tuple[Any, ...]] = []
    size = 0
    for row in page:
        cost = row_chars(row)
        if rows and (len(rows) >= batch_rows or size + cost > INSERT_CHUNK_CHARS):
            yield rows
            rows, size = [], 0
        rows.append(row)
        size += cost
    if rows:
        yield rows


def insert_statement(table: str, row_count: int) -> str:
    """One `INSERT ... VALUES` carrying `row_count` bound tuples."""
    placeholders = ", ".join(["(" + ", ".join("?" for _ in RAW_COLUMNS) + ")"] * row_count)
    return f"INSERT INTO {table} ({INSERT_COLUMNS}) VALUES {placeholders}"


def insert_batch(cursor: SqlExecutor, table: str, rows: Sequence[Sequence[Any]]) -> int:
    """Insert one batch — one statement, one round trip — and return the row count."""
    if not rows:
        return 0
    if len(rows) > MAX_ROWS_PER_STATEMENT:
        raise ValueError(f"a batch carries at most {MAX_ROWS_PER_STATEMENT} rows, got {len(rows)}")
    params = tuple(value for row in rows for value in row)
    if len(params) > MAX_STATEMENT_PARAMETERS:
        raise ValueError(f"a statement carries at most {MAX_STATEMENT_PARAMETERS} parameters, got {len(params)}")
    cursor.execute(insert_statement(table, len(rows)), params)
    return len(rows)


def advance_watermarks(watermarks: dict[int, int], rows: Sequence[Sequence[Any]]) -> None:
    """Move each partition's resume point to the highest offset the batch carried."""
    for row in rows:
        partition = int(row[PARTITION_INDEX])
        watermarks[partition] = max(watermarks.get(partition, -1), int(row[OFFSET_INDEX]))


# --- COPY INTO path -----------------------------------------------------------
# The other way in: write the rows to disk as newline-delimited JSON, upload them
# to a Unity Catalog volume, and let one `COPY INTO` read the whole directory.
#
# It removes both terms of the INSERT cost model. There is no 10,000-parameter
# ceiling to pack against, and no per-row binding to pay for: the warehouse reads
# its own object storage, so the bytes never cross the parameter channel that
# measured 204 K/s. What is left is a fixed per-statement cost, paid once instead
# of 2,353 times.
VOLUME_ENV = "BLUESKY_VOLUME"
STAGING_VOLUME = "staging"

# Files per `COPY INTO` should be large, because each one is a separate list,
# parse and commit task. 200 MB puts a 1.7 M-row backlog in ~10 files, which a
# 2X-Small warehouse can read in parallel without making any one task the
# bottleneck. Below ~16 MB the per-file overhead starts to dominate.
NDJSON_FILE_BYTES = 200 * 1000 * 1000

# Rows per SQLite page during an export. Larger than a batch page because nothing
# downstream has a 10,000-parameter limit; ~8,000 rows is ~8 MB held in memory at
# a time, and cuts the per-page query count by 10x against the INSERT path.
EXPORT_PAGE_ROWS = 8_000

STAGING_PREFIX = "load_"


def volume_fqn() -> str:
    """Fully-qualified staging volume, derived from the table's catalog and schema."""
    if configured := os.environ.get(VOLUME_ENV):
        return configured
    catalog, schema, _ = raw_table_fqn().split(".")
    return f"{catalog}.{schema}.{STAGING_VOLUME}"


def volume_root(fqn: str | None = None) -> str:
    """`/Volumes/catalog/schema/volume` — the path a `COPY INTO` reads."""
    catalog, schema, volume = (fqn or volume_fqn()).split(".")
    return f"/Volumes/{catalog}/{schema}/{volume}"


def ensure_volume(cursor: SqlExecutor, fqn: str | None = None) -> str:
    """Create the staging volume if it does not exist. Returns its fully-qualified name."""
    resolved = fqn or volume_fqn()
    cursor.execute(f"CREATE VOLUME IF NOT EXISTS {resolved};")
    logger.info("Ensured staging volume %s", resolved)
    return resolved


def copy_selection() -> str:
    """The projection `COPY INTO` reads, with each non-string column cast to its target type.

    JSON inference types every integer literal as `BIGINT`, and `COPY INTO` refuses
    to merge `BIGINT` into an `INT` column — measured: `DELTA_FAILED_TO_MERGE_FIELDS`
    on `partition`, with plain inference, with `inferSchema = 'false'`, and with
    `schemaHints` naming the type (that option is silently ignored for JSON). A
    `SELECT` around the file read is the documented way to control it, and casting
    here is deterministic where inference is not: reordering `RAW_COLUMNS` or
    retyping a column cannot silently break the load.
    """
    return ", ".join(
        f"`{name}`" if kind == "STRING" else f"CAST(`{name}` AS {kind}) AS `{name}`"
        for name, kind in RAW_COLUMNS.items()
    )


def copy_into_statement(table: str, source: str) -> str:
    """One `COPY INTO` for a whole directory of NDJSON files.

    `force = 'false'` is the default and is written out on purpose: it is what
    makes a retried run a no-op instead of a duplicate. `COPY INTO` keeps a load
    history keyed by file path, so files already committed are skipped without
    being read. That history is per table and does not survive the table being
    recreated — and it dedupes file identity, never row content, so it is not the
    same guarantee as an upsert.
    """
    return (
        f"COPY INTO {table}\n"
        f"FROM (SELECT {copy_selection()} FROM '{source}')\n"
        "FILEFORMAT = JSON\n"
        "COPY_OPTIONS ('force' = 'false')"
    )


def ndjson_line(row: Sequence[Any]) -> bytes:
    """One row as NDJSON, UTF-8 encoded.

    Keys are the column names, which is how `COPY INTO ... FILEFORMAT = JSON`
    matches fields to columns — by name, not position. `ensure_ascii` stays off so
    non-ASCII payloads are written as UTF-8 rather than `\\uXXXX` escapes, which
    would inflate the file by up to 6x on exactly the emoji-heavy rows this
    firehose carries.
    """
    record = dict(zip(RAW_COLUMNS, row, strict=True))
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def ndjson_overhead_bytes(con: sqlite3.Connection, watermarks: dict[int, int], *, sample: int = 500) -> float:
    """Mean bytes of JSON framing per row: keys, quotes, braces, escaping.

    A dry run has to know how big the export will be before writing it, and the
    answer is not the bound-parameter size: JSON repeats every column name on
    every row and escapes the quotes inside `payload`, which is itself serialised
    JSON. Measured on a sample rather than modelled, because the escaping term
    depends on the payload contents and this firehose's payloads are quote-dense.
    """
    where, params = pending_range(watermarks)
    rows = con.execute(f"SELECT {SELECT_COLUMNS} FROM events WHERE {where} LIMIT ?", [*params, sample]).fetchall()
    if not rows:
        return 0.0
    framed = sum(len(ndjson_line(tuple(row))) for row in rows)
    raw = sum(row_chars(tuple(row)) for row in rows)
    return (framed - raw) / len(rows)


@dataclass(frozen=True)
class ExportedFile:
    """One NDJSON file written to the local work directory."""

    path: Path
    rows: int
    size: int


def export_ndjson(
    con: sqlite3.Connection,
    watermarks: dict[int, int],
    out_dir: Path,
    *,
    file_bytes: int = NDJSON_FILE_BYTES,
    max_rows: int | None = None,
) -> Iterator[ExportedFile]:
    """Write the unloaded rows to NDJSON files of about `file_bytes` each.

    A file is closed when the next row would push it past the target, so files
    are never larger than one row over the target and a single oversized row still
    gets its own file. Files are written in `(partition, offset)` order and named
    by index, which is enough because the whole set goes in one `COPY INTO` and
    `COPY INTO` dedupes by path rather than by content or order.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    handle = None
    path: Path | None = None
    index = 0
    rows = 0
    size = 0
    try:
        for page in _iter_pages(con, watermarks, page_rows=EXPORT_PAGE_ROWS, max_rows=max_rows):
            for row in page:
                if handle is None or path is None:
                    path = out_dir / f"part-{index:05d}.json"
                    handle = path.open("wb")
                line = ndjson_line(row)
                handle.write(line)
                size += len(line)
                rows += 1
                if size >= file_bytes:
                    handle.close()
                    handle = None
                    yield ExportedFile(path, rows, size)
                    index += 1
                    rows = size = 0
        if handle is not None and path is not None:
            handle.close()
            handle = None
            yield ExportedFile(path, rows, size)
    finally:
        if handle is not None:
            handle.close()


class VolumeFiles:
    """The file half of a volume: upload and delete.

    `COPY INTO` needs rows in a volume, and a volume is object storage, so there
    is no SQL statement that writes one. This wraps the Files API — the same thing
    `databricks fs cp` calls — behind the two operations the loader needs, so the
    tests can stand in a recording double.
    """

    def __init__(self, client: Any = None) -> None:
        if client is None:
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
        self._client = client

    def upload(self, remote_path: str, local_path: Path) -> None:
        """Upload one local file, overwriting any file already at that path."""
        with local_path.open("rb") as handle:
            self._client.files.upload(remote_path, handle, overwrite=True)

    def list_dirs(self, remote_dir: str, *, prefix: str = "") -> list[str]:
        """Child directories of `remote_dir` whose name starts with `prefix`, sorted.

        The API returns directory paths **with a trailing slash**, so the basename
        has to strip it first: `rstrip("/").rsplit("/", 1)[-1]`. Taking the last
        segment of the raw path yields an empty string for every directory, which
        makes `prefix` match nothing — measured, and it silently turned
        `clear_staging` into a no-op until a leftover directory was found by hand.
        """
        try:
            entries = self._client.files.list_directory_contents(remote_dir)
            paths = [
                entry.path
                for entry in entries
                if entry.is_directory and entry.path.rstrip("/").rsplit("/", 1)[-1].startswith(prefix)
            ]
        except Exception as exc:  # a volume with nothing in it, or not there at all
            logger.debug("Nothing to list at %s (%s)", remote_dir, exc)
            return []
        return sorted(paths)

    def remove_dir(self, remote_dir: str) -> int:
        """Delete a directory and everything under it; returns the count of files removed.

        Three API facts, all measured, shape this:

        - `files.delete` on a directory raises `NotFound` — it only deletes files.
        - `delete_directory` is **not recursive**: on a non-empty directory it raises
          `BadRequest: The directory is not empty`.
        - So the files have to go first, outer directory last, deepest first.

        A directory that could not be emptied is a warning, not an exception: it is
        stale staging, and failing the load over it would be the wrong trade.
        """
        removed, leftover = self._delete_tree(remote_dir)
        if leftover:
            logger.warning(
                "Leaving %d entr%s under %s: the load does not depend on them, "
                "but they will be retried on the next run.",
                leftover,
                "y" if leftover == 1 else "ies",
                remote_dir,
            )
        return removed

    def _delete_tree(self, remote_dir: str) -> tuple[int, int]:
        """Delete files and subdirectories below `remote_dir`, then the directory itself.

        Returns `(files removed, entries that refused to go)`.
        """
        removed = 0
        leftover = 0
        try:
            entries = list(self._client.files.list_directory_contents(remote_dir))
        except Exception as exc:  # nothing there at all — the normal case on a first run
            logger.debug("Nothing to remove at %s (%s)", remote_dir, exc)
            return 0, 0

        for entry in entries:
            if entry.is_directory:
                nested_removed, nested_left = self._delete_tree(entry.path)
                removed += nested_removed
                leftover += nested_left
            else:
                try:
                    self._client.files.delete(entry.path)
                    removed += 1
                except Exception as exc:
                    logger.debug("Could not delete %s (%s)", entry.path, exc)
                    leftover += 1

        try:
            self._client.files.delete_directory(remote_dir)
        except Exception as exc:
            logger.debug("Could not remove directory %s (%s)", remote_dir, exc)
            leftover += 1
        return removed, leftover


@dataclass(frozen=True)
class CostEstimate:
    """What a load is expected to cost, and how each number was derived."""

    rows: int
    chars: int
    statements: int
    statement_seconds: float
    warehouse_seconds: float
    dbu: float
    usd: float
    details: dict[str, Any] = None  # type: ignore[assignment]

    @property
    def minutes(self) -> float:
        return self.warehouse_seconds / 60


def estimate_cost(rows: int, chars: int, *, auto_stop_seconds: float = AUTO_STOP_SECONDS) -> CostEstimate:
    """Price a load from its row and parameter-byte counts.

    Statements are bounded by both DBSQL caps, so the count is whichever limit is
    hit first — the parameter cap at 769 rows, or byte-packing at ~745 KB on this
    data. Time is the measured fixed cost per statement plus the measured per-byte
    term; cost is warehouse uptime, at the size and list price above, including the
    one-minute scale-to-zero countdown the idling warehouse bills.

    Nothing pending still costs that countdown: a run that finds the backlog
    drained has already connected, created the table and read the watermark, and
    the warehouse bills its idle minute either way.
    """
    if rows <= 0:
        dbu = auto_stop_seconds / 3600 * WAREHOUSE_DBU_PER_HOUR
        return CostEstimate(
            0,
            0,
            0,
            0.0,
            auto_stop_seconds,
            dbu,
            dbu * USD_PER_DBU,
            {
                "warehouse_size": WAREHOUSE_SIZE,
                "dbu_per_hour": WAREHOUSE_DBU_PER_HOUR,
                "usd_per_dbu": USD_PER_DBU,
                "auto_stop_seconds": auto_stop_seconds,
            },
        )
    statements = max(-(-rows // MAX_ROWS_PER_STATEMENT), -(-chars // INSERT_CHUNK_CHARS))
    statement_seconds = statements * STATEMENT_OVERHEAD_SECONDS + chars / PARAMETER_CHARS_PER_SECOND
    warehouse_seconds = statement_seconds + auto_stop_seconds
    dbu = warehouse_seconds / 3600 * WAREHOUSE_DBU_PER_HOUR
    return CostEstimate(
        rows=rows,
        chars=chars,
        statements=statements,
        statement_seconds=statement_seconds,
        warehouse_seconds=warehouse_seconds,
        dbu=dbu,
        usd=dbu * USD_PER_DBU,
        details={
            "avg_row_chars": chars / rows,
            "rows_per_statement": rows / statements,
            "warehouse_size": WAREHOUSE_SIZE,
            "dbu_per_hour": WAREHOUSE_DBU_PER_HOUR,
            "usd_per_dbu": USD_PER_DBU,
            "auto_stop_seconds": auto_stop_seconds,
        },
    )


# --- COPY INTO cost model -----------------------------------------------------
# Measured 2026-10-28 on the same warehouse, by timing one `COPY INTO` over
# 24.7 MB, 149.8 MB and 534.7 MB of this loader's own NDJSON. Least squares over
# the three gives **3.30 s fixed + 126 MB/s** (residuals +0.18, -0.23, +0.06 s).
#
# A `COPY INTO` is one statement, so it pays a fixed cost as an INSERT does —
# planning and a Delta commit. What it does not pay is the per-parameter binding,
# which is the entire point: the byte term here is 126 MB/s against the INSERT
# path's 204 K/s, a factor of ~620. The warehouse reads its own object storage;
# nothing crosses the parameter channel.
COPY_STATEMENT_OVERHEAD_SECONDS = 3.30
COPY_SOURCE_BYTES_PER_SECOND = 126_000_000

# Client-side, and therefore wall time rather than money: writing the NDJSON and
# pushing it into the volume cost nothing in DBU, but they are what the operator
# actually waits for. Measured locally, so they are properties of this machine
# and this uplink rather than of the platform — the one number that will differ
# on another host.
#
# The upload figure is the reason a full backfill takes 13 minutes of wall time
# for 37 s of warehouse work. The Files API put 2.26 GB in 698 s (3.2 MB/s) over
# the whole run, and 7.7 MB/s on the smaller probes; single-threaded per file and
# dominated by round trips, so it does not improve with file size. Parallel
# uploads are the obvious fix if this ever runs routinely; it is not worth the
# complexity for a one-off backfill.
EXPORT_BYTES_PER_SECOND = 69_000_000
UPLOAD_BYTES_PER_SECOND = 7_700_000


@dataclass(frozen=True)
class CopyEstimate:
    """What a `COPY INTO` load is expected to cost, and how each number was derived."""

    rows: int
    file_bytes: int
    files: int
    statements: int
    warehouse_seconds: float
    dbu: float
    usd: float
    export_seconds: float
    upload_seconds: float
    details: dict[str, Any] = None  # type: ignore[assignment]

    @property
    def minutes(self) -> float:
        return self.warehouse_seconds / 60

    @property
    def wall_seconds(self) -> float:
        """Local export plus upload plus the warehouse work — what the operator waits."""
        return self.export_seconds + self.upload_seconds + self.warehouse_seconds


def estimate_copy_cost(
    rows: int,
    file_bytes: int,
    *,
    files: int | None = None,
    auto_stop_seconds: float = AUTO_STOP_SECONDS,
) -> CopyEstimate:
    """Price a `COPY INTO` load from its row count and the size of the export.

    One statement covers every file, so the statement count is fixed at one —
    unless the backlog is empty, in which case the run still connects, reads the
    watermark and pays the scale-to-zero countdown, exactly as the INSERT path
    does.
    """
    if rows <= 0:
        dbu = auto_stop_seconds / 3600 * WAREHOUSE_DBU_PER_HOUR
        return CopyEstimate(0, 0, 0, 0, auto_stop_seconds, dbu, dbu * USD_PER_DBU, 0.0, 0.0, {"empty": True})
    file_count = files if files is not None else -(-file_bytes // NDJSON_FILE_BYTES)
    warehouse_seconds = COPY_STATEMENT_OVERHEAD_SECONDS + file_bytes / COPY_SOURCE_BYTES_PER_SECOND + auto_stop_seconds
    dbu = warehouse_seconds / 3600 * WAREHOUSE_DBU_PER_HOUR
    return CopyEstimate(
        rows=rows,
        file_bytes=file_bytes,
        files=file_count,
        statements=1,
        warehouse_seconds=warehouse_seconds,
        dbu=dbu,
        usd=dbu * USD_PER_DBU,
        export_seconds=file_bytes / EXPORT_BYTES_PER_SECOND,
        upload_seconds=file_bytes / UPLOAD_BYTES_PER_SECOND,
        details={
            "warehouse_size": WAREHOUSE_SIZE,
            "dbu_per_hour": WAREHOUSE_DBU_PER_HOUR,
            "usd_per_dbu": USD_PER_DBU,
            "auto_stop_seconds": auto_stop_seconds,
            "bytes_per_row": file_bytes / rows,
        },
    )
