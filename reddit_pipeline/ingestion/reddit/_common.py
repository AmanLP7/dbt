"""Shared scaffolding for the Reddit extractors.

Holds what the three extractors would otherwise duplicate: the tuning constants,
environment validation, the Reddit and Databricks clients, the idempotent target
table setup, and the two parameterised incremental load strategies (rolling
window and key-based upsert).

Importing this module loads `.env` and does nothing else: no Reddit API call, no
Databricks connection.

Every value that reaches SQL goes through a `?` parameter. The only things ever
interpolated into a statement are identifiers (catalog, schema, table and column
names), which cannot be bound.
"""

from __future__ import annotations

import datetime
import logging
import os
import time
from collections import OrderedDict
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import praw
from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor

# Rolling window (hours) on an entity's own created_utc: rows older than this are
# left untouched, so posts and comments accumulate history across runs.
INCREMENTAL_WINDOW_HOURS = 5

SUBREDDIT = "worldnews"

CATALOG = "workspace"
SCHEMA = "topic_raw"

# Bounds on the source fetch: how many hot posts to walk, how many comments to
# take per post for the comments table, and how many user profiles to collect.
POSTS_LIMIT = 1000
COMMENTS_PER_POST = 10
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


def window_cutoff() -> float:
    """Most recent epoch second the rolling window keeps, computed once per run."""
    return time.time() - INCREMENTAL_WINDOW_HOURS * 3600


def load_timestamp() -> datetime.datetime:
    """Load-time timestamp stamped on every row of a run, timezone-aware UTC."""
    return datetime.datetime.now(datetime.timezone.utc)


def ensure_table(cursor: Cursor, table: str, columns: OrderedDict[str, str]) -> None:
    """Create the catalog, schema and table if they do not exist yet."""
    cursor.execute(f"CREATE CATALOG IF NOT EXISTS {CATALOG};")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA};")
    column_defs = ",\n        ".join(f"{name} {type_name}" for name, type_name in columns.items())
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n        {column_defs}\n    );")


def ensure_columns(cursor: Cursor, table: str, columns: OrderedDict[str, str]) -> None:
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


def _insert_sql(table: str, columns: OrderedDict[str, str]) -> str:
    """Build the parameterised bulk insert for a table's column mapping."""
    names = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {table} ({names}) VALUES ({placeholders})"


def replace_window(
    cursor: Cursor,
    table: str,
    columns: OrderedDict[str, str],
    rows: Sequence[tuple[Any, ...]],
    cutoff_col: str,
    cutoff: float,
) -> int:
    """Replace the rolling window on `cutoff_col` and insert `rows`.

    Does nothing when there is nothing to insert — a delete with no insert would
    drop rows that no fetch can restore. Returns the number of rows written.
    """
    if not rows:
        logger.info("No rows to load into %s; skipping delete and insert.", table)
        return 0
    cursor.execute(f"DELETE FROM {table} WHERE {cutoff_col} >= ?;", (cutoff,))
    cursor.executemany(_insert_sql(table, columns), rows)
    return len(rows)


def replace_by_key(
    cursor: Cursor,
    table: str,
    columns: OrderedDict[str, str],
    rows: Sequence[tuple[Any, ...]],
    key: str,
) -> int:
    """Upsert `rows` by `key`: delete the fetched keys, then insert the rows.

    Does nothing when there is nothing to insert. Returns the number of rows
    written.
    """
    if not rows:
        logger.info("No rows to load into %s; skipping delete and insert.", table)
        return 0
    key_index = list(columns).index(key)
    keys = tuple(row[key_index] for row in rows)
    placeholders = ", ".join("?" for _ in keys)
    cursor.execute(f"DELETE FROM {table} WHERE {key} IN ({placeholders});", keys)
    cursor.executemany(_insert_sql(table, columns), rows)
    return len(rows)
