"""Shared scaffolding for the Reddit extractors.

Holds what the three extractors would otherwise duplicate: the tuning constants,
the subreddit list (read from the dbt section seed), environment validation, the
Reddit and Databricks clients, the idempotent target table setup, and the
parameterised incremental load strategy.

Importing this module loads `.env` and does nothing else: no Reddit API call, no
Databricks connection.

Every value that reaches SQL goes through a `?` parameter. The only things ever
interpolated into a statement are identifiers (catalog, schema, table and column
names), which cannot be bound.
"""

from __future__ import annotations

import csv
import datetime
import json
import logging
import os
import pathlib
import sqlite3
import time
from collections import OrderedDict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import praw
from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor


class Submission(Protocol):
    """The listing fields the extractors read off a PRAW submission.

    A protocol rather than PRAW's own type: the extractors only touch these
    fields, and a test double then stands in without a network.
    """

    id: str
    title: str
    score: int
    url: str
    num_comments: int
    created_utc: float
    selftext: str
    author: Any
    comments: Any
    subreddit: Any


class SqlExecutor(Protocol):
    """The two cursor methods the loaders use, so a test double can stand in.

    Typing these against the connector's `Cursor` would force every test to build
    a real one; the loaders only ever `execute` and `fetchall`.
    """

    def execute(self, operation: str, parameters: Any = None) -> Any: ...

    def fetchall(self) -> Sequence[Sequence[Any]]: ...


# Rolling window (hours) on an entity's own created_utc: a run loads the rows it
# fetched inside this window and ignores older ones. 26 rather than 24 so a daily
# build always covers the previous calendar day with slack; `macros/window_hours.sql`
# repeats it for the dbt side and `tests/test_window_contract.py` pins them together.
INCREMENTAL_WINDOW_HOURS = 26

CATALOG = "workspace"
SCHEMA = "topic_raw"

# The subreddit list is not a constant here: it is `seeds/sections.csv`, the same
# file that maps a subreddit to its newspaper section. Adding a subreddit is a data
# change in one file, and the ingest cannot drift from the seed the marts join on.
SECTION_SEED = pathlib.Path(__file__).resolve().parents[2] / "seeds" / "sections.csv"

# Two bounded listings per subreddit, `new` for what is being posted right now
# (the volume and recency the daily-volume mart measures) and `top(day)` for the
# stories that have already earned their place on the page. PRAW pages a listing
# only when asked for more than one request's worth, so each listing costs one
# request whatever the item cap: two per subreddit, 132 for today's seed.
NEW_POSTS_LIMIT = 100
TOP_POSTS_LIMIT = 100
TOP_TIME_FILTER = "day"

# Comments cost one request per post, so they are fetched for the most-commented
# posts of the run rather than every post: 200 requests against a 6,000/hour
# budget, and the edition's engagement comes from the listing's own comment
# count, not from this table.
COMMENT_POST_LIMIT = 200
COMMENTS_PER_POST = 10

# Users cost one request per post whose comment forest is walked, plus one per
# profile read. `USER_POST_LIMIT` is how many posts one subreddit's listing
# yields; `USER_POST_WALK_LIMIT` bounds the comment-forest walks across the whole
# run, so the expensive phase can look at several subreddits instead of spending
# its budget on the first one.
USER_POST_LIMIT = 50
USER_POST_WALK_LIMIT = 200
USER_COMMENTS_PER_POST = 15
TARGET_USER_COUNT = 200

REQUIRED_ENV = (
    "REDDIT_CLIENT_ID",
    "REDDIT_CLIENT_SECRET",
    "REDDIT_USER_AGENT",
    "DATABRICKS_HOSTNAME",
    "DATABRICKS_HTTP_PATH",
)

# Where the extractors dump a run's rows and `copy_into` reads them back: one
# SQLite file per run, under `reddit_pipeline/data/` by default. Overridable so
# an operator can point a one-off at a scratch path.
REDDIT_SQLITE_PATH_ENV = "REDDIT_SQLITE_PATH"
DEFAULT_SQLITE_PATH = pathlib.Path(__file__).resolve().parents[2] / "data" / "reddit.db"

# Column name -> SQL type, in table order. `ingested_at` is last in every table:
# the UTC timestamp the row was loaded, not a value Reddit supplied.
POSTS_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("id", "STRING"),
        ("title", "STRING"),
        ("score", "INT"),
        ("url", "STRING"),
        ("comms_num", "INT"),
        ("created_utc", "DOUBLE"),
        ("body", "STRING"),
        ("author", "STRING"),
        ("subreddit", "STRING"),
        ("ingested_at", "TIMESTAMP"),
    ]
)

COMMENTS_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("id", "STRING"),
        ("post_id", "STRING"),
        ("body", "STRING"),
        ("score", "INT"),
        ("author", "STRING"),
        ("created_utc", "DOUBLE"),
        ("parent_id", "STRING"),
        ("subreddit", "STRING"),
        ("ingested_at", "TIMESTAMP"),
    ]
)

USERS_COLUMNS: OrderedDict[str, str] = OrderedDict(
    [
        ("username", "STRING"),
        ("id", "STRING"),
        ("created_utc", "DOUBLE"),
        ("link_karma", "INT"),
        ("comment_karma", "INT"),
        ("is_employee", "BOOLEAN"),
        ("is_gold", "BOOLEAN"),
        ("ingested_at", "TIMESTAMP"),
    ]
)

# The three raw tables the extractors dump and `copy_into` lands, in the order a
# `COPY INTO` run writes them.
RAW_TABLE_COLUMNS: dict[str, OrderedDict[str, str]] = {
    "r_posts": POSTS_COLUMNS,
    "r_comments": COMMENTS_COLUMNS,
    "r_users": USERS_COLUMNS,
}

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def configure_logging() -> None:
    """Configure root logging once per process; `LOG_LEVEL` overrides INFO."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def validate_env() -> None:
    """Raise if any required variable is unset, empty, or still a `your_` placeholder."""
    bad_vars = [var for var in REQUIRED_ENV if not (value := os.getenv(var)) or "your_" in value]
    if bad_vars:
        raise RuntimeError(
            f"Missing or default environment variables found: {bad_vars}. "
            "Please configure your credentials in the .env file."
        )


def reddit_client() -> praw.Reddit:
    """Build the read-only PRAW client from the validated `.env` credentials."""
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT"),
    )


def databricks_config() -> Config:
    """The Databricks CLI login from `~/.databrickscfg`, which refreshes itself.

    `auth_type = databricks-cli` makes the SDK return the CLI's cached OAuth token
    and run the CLI's own refresh when it has expired. That is why the extractors
    never pass a token. The dbt step does need one in `DATABRICKS_TOKEN`, but mints
    it from this same config per run (`databricks_token()`) rather than trusting a
    value that may have been sitting in `.env` since the last login.
    """
    return Config(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))


def databricks_token() -> str:
    """A freshly minted bearer token, for callers that want a token, not a provider.

    `profiles/profiles.yml` hands dbt a `DATABRICKS_TOKEN` and the dbt CLI has no
    way to accept a credentials provider, so the orchestrator derives one here at
    run time rather than letting the dbt subprocess inherit whatever the process
    started with.
    """
    return databricks_config().authenticate()["Authorization"].split(" ", 1)[1]


def connect() -> tuple[Connection, Cursor]:
    """Open a Databricks SQL connection and its cursor.

    Credentials come from the CLI profile in `~/.databrickscfg` — the OAuth token
    saved by `databricks auth login` — not from a PAT in .env. Pass a *factory*:
    the connector calls it once to obtain a header factory, then calls that per
    request, so `credentials_provider=config.authenticate` alone fails with
    "'dict' object is not callable".
    """
    config = databricks_config()
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        credentials_provider=lambda: config.authenticate,
    )
    return connection, connection.cursor()


def full_table(name: str) -> str:
    """Qualify a raw table name with the catalog and schema."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def subreddit_list(seed: pathlib.Path = SECTION_SEED) -> tuple[str, ...]:
    """The subreddits to ingest, in seed order, read from `seeds/sections.csv`.

    The section seed is the single source of the list: it already names every
    subreddit the newspaper prints, and reading it here means adding a subreddit
    stays a one-file data change that cannot drift between ingest and marts. The
    names are lower-case there and in the raw table, so the join needs no case
    folding; Reddit itself is case-insensitive.
    """
    with seed.open(newline="", encoding="utf-8") as handle:
        return tuple(row["subreddit"].strip() for row in csv.DictReader(handle) if row["subreddit"].strip())


def subreddit_listings(client: praw.Reddit, name: str) -> Iterator[Submission]:
    """The two bounded listings this pipeline reads for one subreddit.

    `new` is the firehose and `top(day)` the already-earned page, so a story that
    is rising now and one that peaked this morning are both seen. Two requests
    per subreddit, whatever the traffic.
    """
    subreddit = client.subreddit(name)
    yield from subreddit.new(limit=NEW_POSTS_LIMIT)
    yield from subreddit.top(limit=TOP_POSTS_LIMIT, time_filter=TOP_TIME_FILTER)


def harvest_submissions(client: praw.Reddit, names: Sequence[str]) -> tuple[list[Submission], list[str]]:
    """Every listing post from `names`, deduped by id, plus the names that failed.

    A post appears in both listings, so the dict keeps the later one: `top` runs
    second and carries the fresher score. One unreadable subreddit — private,
    banned, renamed, or rate-limited — is logged and skipped rather than sinking a
    run that has already spent requests on the others; the failures are returned
    so the caller can report how many of the seed's subreddits it actually read.
    """
    posts: dict[str, Submission] = {}
    failed: list[str] = []
    for name in names:
        try:
            for submission in subreddit_listings(client, name):
                posts[submission.id] = submission
        except Exception as exc:  # private, banned, renamed, or rate limited
            failed.append(name)
            logger.warning("Skipping r/%s: %s", name, exc)
    return list(posts.values()), failed


def row_values(columns: OrderedDict[str, str], record: dict[str, Any]) -> tuple[Any, ...]:
    """The positional row for `columns`, taken from `record` by name.

    Building the tuple by name rather than by hand keeps a column inserted in the
    middle of the mapping from silently shifting every value after it.
    """
    return tuple(record[name] for name in columns)


def window_cutoff() -> float:
    """Oldest epoch second a run will write, computed once per run (a read filter)."""
    return time.time() - INCREMENTAL_WINDOW_HOURS * 3600


def load_timestamp() -> datetime.datetime:
    """Load-time timestamp stamped on every row of a run, timezone-aware UTC."""
    return datetime.datetime.now(datetime.timezone.utc)


def ensure_table(cursor: SqlExecutor, table: str, columns: OrderedDict[str, str]) -> None:
    """Create the catalog, schema and table if they do not exist yet."""
    cursor.execute(f"CREATE CATALOG IF NOT EXISTS {CATALOG};")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA};")
    column_defs = ",\n        ".join(f"{name} {type_name}" for name, type_name in columns.items())
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n        {column_defs}\n    );")


def ensure_columns(cursor: SqlExecutor, table: str, columns: OrderedDict[str, str]) -> None:
    """Additively migrate a pre-existing table to the given column mapping.

    Idempotent: a table that already has every column is left alone, and a table
    missing one (e.g. `ingested_at`, added after the table first shipped) gets one
    `ALTER TABLE ... ADD COLUMNS` per missing column.
    """
    cursor.execute(f"DESCRIBE TABLE {table};")
    existing = {str(row[0]).lower() for row in cursor.fetchall()}
    for name, type_name in columns.items():
        if name.lower() not in existing:
            logger.info("Adding missing column %s.%s.", table, name)
            cursor.execute(f"ALTER TABLE {table} ADD COLUMNS ({name} {type_name});")


# --- SQLite dump path ---------------------------------------------------------
# The extractors no longer write to Databricks directly. A run fetches from
# Reddit and dumps every fetched row into one local SQLite database, and the
# `copy_into` module exports that dump to NDJSON and lands it with `COPY INTO`.
# The dbt staging models dedupe by id, so the raw tables are append-only.


def sqlite_path() -> Path:
    """The SQLite database the extractors write and `copy_into` reads.

    `REDDIT_SQLITE_PATH`, else `reddit_pipeline/data/reddit.db`.
    """
    return Path(os.environ.get(REDDIT_SQLITE_PATH_ENV, DEFAULT_SQLITE_PATH)).expanduser()


def connect_sqlite(path: Path | None = None, *, reset: bool = False) -> sqlite3.Connection:
    """Open the run's SQLite database, creating the file and parent directory.

    `reset=True` starts a fresh file, so an extractor run replaces rather than
    appends to the previous run's rows.
    """
    resolved = path or sqlite_path()
    if reset:
        resolved.unlink(missing_ok=True)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(resolved)


def ensure_sqlite_table(con: sqlite3.Connection, table: str, columns: OrderedDict[str, str]) -> None:
    """Create the SQLite table if it does not exist, mapping Databricks types to SQLite."""
    sqlite_types = {"STRING": "TEXT", "INT": "INTEGER", "DOUBLE": "REAL", "BOOLEAN": "INTEGER", "TIMESTAMP": "TEXT"}
    column_defs = ", ".join(f"{name} {sqlite_types[type_name]}" for name, type_name in columns.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({column_defs})")
    con.commit()


def dump_rows(
    con: sqlite3.Connection,
    table: str,
    columns: OrderedDict[str, str],
    rows: Sequence[Sequence[Any]],
    *,
    replace: bool = False,
) -> int:
    """Write `rows` into the SQLite table in one `executemany`, converting values first.

    The dump stores the values the way SQLite keeps them: a `TIMESTAMP` that is
    still a `datetime` is rendered as an ISO-8601 UTC string, and a `BOOLEAN` is
    collapsed to `0`/`1`. Everything else passes through as-is. `replace=True`
    clears the table first, which is how a fresh run overwrites the previous one.
    """
    if replace:
        con.execute(f"DELETE FROM {table}")
    names = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    converted: list[tuple[Any, ...]] = []
    for row in rows:
        values: list[Any] = []
        for value, type_name in zip(row, columns.values(), strict=True):
            if type_name == "TIMESTAMP" and isinstance(value, datetime.datetime):
                values.append(value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
            elif type_name == "BOOLEAN":
                values.append(int(bool(value)))
            else:
                values.append(value)
        converted.append(tuple(values))
    con.executemany(f"INSERT INTO {table} ({names}) VALUES ({placeholders})", converted)
    con.commit()
    return len(rows)


# --- COPY INTO machinery ------------------------------------------------------
# The other half of the write path: export the SQLite dump to newline-delimited
# JSON, upload the files to a Unity Catalog volume, and land each table with one
# `COPY INTO`. Nothing crosses the SQL parameter channel; the warehouse reads its
# own object storage.
REDDIT_VOLUME_ENV = "REDDIT_VOLUME"
STAGING_VOLUME = "staging"
STAGING_PREFIX = "load_"

# Files per `COPY INTO` should be large, because each one is a separate list,
# parse and commit task. 200 MB keeps a run in a handful of files, which a
# small warehouse can read in parallel without making any one task the
# bottleneck. Below ~16 MB the per-file overhead starts to dominate.
NDJSON_FILE_BYTES = 200 * 1000 * 1000


def volume_fqn() -> str:
    """Fully-qualified staging volume, derived from the raw schema."""
    if configured := os.environ.get(REDDIT_VOLUME_ENV):
        return configured
    return f"{CATALOG}.{SCHEMA}.{STAGING_VOLUME}"


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


def copy_selection(columns: OrderedDict[str, str]) -> str:
    """The projection `COPY INTO` reads, with each non-string column cast to its target type.

    JSON inference types every integer literal as `BIGINT`, and `COPY INTO` refuses
    to merge `BIGINT` into an `INT` column. A `SELECT` around the file read is the
    documented way to control it, and casting here is deterministic where
    inference is not.
    """
    return ", ".join(
        f"`{name}`" if kind == "STRING" else f"CAST(`{name}` AS {kind}) AS `{name}`" for name, kind in columns.items()
    )


def copy_into_statement(table: str, source: str, columns: OrderedDict[str, str]) -> str:
    """One `COPY INTO` for a whole directory of NDJSON files.

    `force = 'false'` is the default and is written out on purpose: it is what
    makes a retried run a no-op instead of a duplicate. `COPY INTO` keeps a load
    history keyed by file path, so files already committed are skipped without
    being read.
    """
    return (
        f"COPY INTO {table}\n"
        f"FROM (SELECT {copy_selection(columns)} FROM '{source}')\n"
        "FILEFORMAT = JSON\n"
        "COPY_OPTIONS ('force' = 'false')"
    )


def json_value(value: Any, sql_type: str) -> Any:
    """Coerce one value to the JSON literal `COPY INTO` will read back as `sql_type`.

    `TIMESTAMP` values were already rendered to ISO strings by `dump_rows`, so they
    pass through as text; `BOOLEAN` was stored as `0`/`1`, so it is turned back
    into a JSON boolean; `INT` and `DOUBLE` become JSON numbers.
    """
    if value is None:
        return None
    if sql_type == "BOOLEAN":
        return bool(value)
    if sql_type == "INT":
        return int(value)
    if sql_type == "DOUBLE":
        return float(value)
    return value  # STRING and TIMESTAMP are already strings


def ndjson_line(columns: OrderedDict[str, str], row: Sequence[Any]) -> bytes:
    """One row as NDJSON, UTF-8 encoded.

    Keys are the column names, which is how `COPY INTO ... FILEFORMAT = JSON`
    matches fields to columns — by name, not position. `ensure_ascii` stays off so
    non-ASCII payloads are written as UTF-8 rather than `\\uXXXX` escapes.
    """
    record = {name: json_value(value, kind) for (name, kind), value in zip(columns.items(), row, strict=True)}
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


@dataclass(frozen=True)
class ExportedFile:
    """One NDJSON file written to the local work directory."""

    path: Path
    rows: int
    size: int


def export_ndjson(
    con: sqlite3.Connection,
    table: str,
    columns: OrderedDict[str, str],
    out_dir: Path,
    *,
    file_bytes: int = NDJSON_FILE_BYTES,
) -> Iterator[ExportedFile]:
    """Write every row of `table` to NDJSON files of about `file_bytes` each.

    The dump is a fresh per-run artifact, so there is no watermark or paging: the
    whole table is one `SELECT *`. A file is closed when the next row would push
    it past the target, so files are never larger than one row over the target and
    a single oversized row still gets its own file.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    handle = None
    path: Path | None = None
    index = 0
    rows = 0
    size = 0
    try:
        for row in con.execute(f"SELECT * FROM {table}"):
            if handle is None or path is None:
                path = out_dir / f"part-{index:05d}.json"
                handle = path.open("wb")
            line = ndjson_line(columns, row)
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
