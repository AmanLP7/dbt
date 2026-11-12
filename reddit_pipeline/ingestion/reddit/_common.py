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
import logging
import os
import pathlib
import time
from collections import OrderedDict
from collections.abc import Iterator, Sequence
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


# `executemany` sends one statement per row: measured against this warehouse it
# cost 2.00 s per comment row and 1.68 s per user row, so a 200-user load spent
# five and a half minutes on round trips. Rows are instead batched into
# multi-row INSERTs, and both bounds below apply. The row cap keeps the
# statement readable and the parameter count low; the byte cap is the server's
# own limit, which a single large body can exhaust on its own — DBSQL rejects a
# statement whose bound parameters total more than _PARAMETER_BYTES. A row
# bigger than the whole budget still goes alone; truncating it would break the
# raw table's fidelity contract.
INSERT_CHUNK_ROWS = 50
_PARAMETER_BYTES = 1_048_576
INSERT_CHUNK_CHARS = _PARAMETER_BYTES * 3 // 4

# A key delete carries one parameter per id, so it is bounded by the same
# parameter limit but nothing else: 5,000 short ids stay far inside it, and a run
# reading every subreddit in the seed needs a handful of statements rather than
# one per row.
DELETE_CHUNK_ROWS = 5_000


def _row_bytes(row: Sequence[Any]) -> int:
    """Approximate the parameter cost of one row, in UTF-8 bytes.

    The server's limit is bytes, not characters, and a comment body is user text:
    a single emoji is four bytes. Counting `len()` instead understates a
    non-ASCII row by up to 4×, and the batch that follows then trips the limit.
    Non-strings are ids, scores and timestamps — a constant few bytes each.
    """
    return sum(len(value.encode()) if isinstance(value, str) else 8 for value in row)


def chunk_rows(rows: Sequence[Sequence[Any]]) -> list[list[Sequence[Any]]]:
    """Split rows into chunks that respect both the row and the parameter budget."""
    chunks: list[list[Sequence[Any]]] = []
    current: list[Sequence[Any]] = []
    size = 0
    for row in rows:
        cost = _row_bytes(row)
        if current and (len(current) >= INSERT_CHUNK_ROWS or size + cost > INSERT_CHUNK_CHARS):
            chunks.append(current)
            current, size = [], 0
        current.append(row)
        size += cost
    if current:
        chunks.append(current)
    return chunks


def _insert_sql(table: str, columns: OrderedDict[str, str], row_count: int) -> str:
    """Build one INSERT carrying `row_count` value tuples for a column mapping."""
    names = ", ".join(columns)
    placeholders = ", ".join([f"({', '.join('?' for _ in columns)})"] * row_count)
    return f"INSERT INTO {table} ({names}) VALUES {placeholders}"


def _insert_rows(
    cursor: SqlExecutor,
    table: str,
    columns: OrderedDict[str, str],
    rows: Sequence[Sequence[Any]],
) -> None:
    """Insert `rows` in as few statements as the row and parameter budgets allow."""
    for chunk in chunk_rows(rows):
        params = tuple(value for row in chunk for value in row)
        cursor.execute(_insert_sql(table, columns, len(chunk)), params)


def replace_by_key(
    cursor: SqlExecutor,
    table: str,
    columns: OrderedDict[str, str],
    rows: Sequence[tuple[Any, ...]],
    key: str,
) -> int:
    """Upsert `rows` by `key`: delete the fetched keys, then insert the rows.

    Every raw load is an upsert on the entity's own id, never a window rewrite.
    A `delete where created_utc >= cutoff` scope cannot be exact once a run reads
    several subreddits through bounded listings: two consecutive runs cover
    different spans of the same day, so the second deletes rows the first wrote
    and never refetches them. Deleting exactly the ids fetched this run is
    lossless whatever the run's coverage, and it keeps one row per entity instead
    of one per run. `stg_reddit_*` still dedupes, as the at-least-once case —
    rows committed whose acknowledgement was lost — can still land twice.

    Each statement autocommits and there is no transaction spanning the two, so
    the delete and the insert are *interleaved* by chunk rather than run as two
    passes: delete a chunk, write that chunk, next chunk. Deleting everything
    first would let a single failed INSERT leave the whole run's ids gone, and a
    daily run only reads the last 26 hours, so rows older than that at delete
    time would never come back. Interleaved, a failure can strand at most one
    chunk — and that chunk re-fetches on the next run.

    Does nothing when there is nothing to insert: a delete with no insert would
    drop rows that no fetch can restore. Returns the number of rows written.
    """
    if not rows:
        logger.info("No rows to load into %s; skipping delete and insert.", table)
        return 0
    key_index = list(columns).index(key)
    for start in range(0, len(rows), DELETE_CHUNK_ROWS):
        group = rows[start : start + DELETE_CHUNK_ROWS]
        keys = tuple(row[key_index] for row in group)
        placeholders = ", ".join("?" for _ in keys)
        cursor.execute(f"DELETE FROM {table} WHERE {key} IN ({placeholders});", keys)
        _insert_rows(cursor, table, columns, group)
    return len(rows)
