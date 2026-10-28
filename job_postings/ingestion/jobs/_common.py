"""Scaffolding shared by the job-postings extractors.

The keyless job APIs this pipeline reads are *firehoses*: they expose no
role/keyword query, so the extractors fetch whatever the source returns and
land it verbatim in a Delta raw table. Role filtering, HTML stripping and
normalisation happen downstream in dbt (``models/staging/jobs/`` and
``models/marts/jobs/``), never in Python — the raw table stays a faithful
record of what the source said.

Raw is append-only. Each run stamps ``ingested_at`` and inserts; a posting that
scrolls off a source's rolling window simply stops appearing, so history
accumulates instead of being overwritten. Staging dedupes to the newest row per
``(source, source_job_id)``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any

from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)

# Load environment variables from .env, matching the sibling project: the
# Databricks connection settings (and LOG_LEVEL) can live in a gitignored .env
# instead of being exported by hand.
load_dotenv()

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "job_postings"
RAW_TABLE = "raw_job_postings"

# Connection settings. Deliberately no token: the Databricks CLI login in
# ~/.databrickscfg supplies the credentials and refreshes them — see
# databricks_config().
REQUIRED_ENV = ("DATABRICKS_HOSTNAME", "DATABRICKS_HTTP_PATH")

# A value copied straight out of .env.example must not pass for a real setting: a
# `your_workspace_host` host only fails deep inside the connector, with an error
# that says nothing about the missing configuration.
PLACEHOLDER_MARKER = "your_"

# Column order of the raw table and of every insert issued against it.
RAW_COLUMNS: Sequence[str] = (
    "source",
    "source_job_id",
    "title",
    "company_name",
    "location",
    "job_type",
    "industry",
    "salary_text",
    "tags_json",
    "description",
    "url",
    "published_at",
    "ingested_at",
)

_CREATE_TABLE_SQL = """
create table if not exists {fqn} (
    source string,
    source_job_id string,
    title string,
    company_name string,
    location string,
    job_type string,
    industry string,
    salary_text string,
    tags_json string,
    description string,
    url string,
    published_at timestamp,
    ingested_at timestamp
) using delta
"""

_INSERT_PREFIX = "insert into {fqn} (" + ", ".join(RAW_COLUMNS) + ") values "
_ROW_PLACEHOLDER = "(" + ", ".join(["?"] * len(RAW_COLUMNS)) + ")"

# databricks-sql-connector issues one statement per parameter set for
# `INSERT ... VALUES`, so executemany() costs a round trip *per row* — measured
# at ~2s each, i.e. minutes for a few hundred postings. Packing many value
# tuples into a single statement converts N round trips into N / this.
INSERT_CHUNK_ROWS = 50

# ...but a row count alone is the wrong bound. DBSQL rejects a statement whose
# parameters total more than _PARAMETER_CHARS, and a single posting's
# `description` is often tens of KB of source HTML, so 50 of them overflow it: a
# live run died with "combined size of parameters is 2059161, exceeding the
# limit of 1048576". A chunk is therefore whatever fits both limits, rows first.
# One row bigger than the budget still goes alone — splitting the string would
# corrupt the raw table's fidelity contract.
_PARAMETER_CHARS = 1_048_576
INSERT_CHUNK_CHARS = _PARAMETER_CHARS * 3 // 4


def utcnow() -> datetime:
    """Naive UTC timestamp; the connector rejects tz-aware datetimes."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def configure_logging() -> None:
    """Configure root logging once per process; `LOG_LEVEL` overrides INFO."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # The SQL connector logs a line per HTTP poll at INFO, which drowns out the
    # per-source summaries the extractors exist to emit.
    logging.getLogger("databricks").setLevel(logging.WARNING)


def require_env() -> dict[str, str]:
    """Return the Databricks connection settings, or raise naming what is unusable."""
    missing = [key for key in REQUIRED_ENV if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "Missing required environment variable(s): "
            + ", ".join(missing)
            + ". Export the Databricks connection settings, e.g.\n"
            "  export DATABRICKS_HOSTNAME=https://<workspace>.cloud.databricks.com\n"
            "  export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>\n"
            "  databricks auth login --host https://<workspace>.cloud.databricks.com"
        )
    placeholders = [key for key in REQUIRED_ENV if PLACEHOLDER_MARKER in os.environ[key]]
    if placeholders:
        raise RuntimeError(
            "Environment variable(s) still hold .env.example placeholders: "
            + ", ".join(placeholders)
            + ". Copy .env.example to .env and fill in the real Databricks values."
        )
    return {key: os.environ[key] for key in REQUIRED_ENV}


def raw_table_fqn() -> str:
    """Fully-qualified name of the raw table, overridable by env."""
    catalog = os.environ.get("JOB_POSTINGS_CATALOG", DEFAULT_CATALOG)
    schema = os.environ.get("JOB_POSTINGS_SCHEMA", DEFAULT_SCHEMA)
    return f"{catalog}.{schema}.{RAW_TABLE}"


def databricks_config() -> Config:
    """The Databricks CLI login from `~/.databrickscfg`, which refreshes itself.

    `auth_type = databricks-cli` makes the SDK return the CLI's cached OAuth token
    and run the CLI's own refresh when it has expired. That is the whole point of
    not taking a token from the environment: an exported OAuth token lives one
    hour, so a long-lived process — a Dagster code server, say — that captured one
    at startup opens every session after the first hour with a dead credential.
    """
    return Config(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))


def databricks_token() -> str:
    """A freshly minted bearer token, for callers that want a token, not a provider.

    `profiles/profiles.yml` hands dbt a `DATABRICKS_TOKEN` and the dbt CLI has no
    way to accept a credentials provider, so the orchestrator derives one here at
    run time rather than letting the dbt subprocess inherit whatever the process
    started with.
    """
    return databricks_config().authenticate()["Authorization"].split(" ", 1)[1]


def connect(env: dict[str, str]):
    """Open a SQL warehouse connection from the validated environment.

    Credentials come from the CLI profile, never from `DATABRICKS_TOKEN`, so a run
    cannot inherit an expired token. Pass a *factory*: the connector calls it once
    to obtain a header factory and then calls that per request, so
    `credentials_provider=config.authenticate` alone fails with
    "'dict' object is not callable".
    """
    host = env["DATABRICKS_HOSTNAME"]
    for prefix in ("https://", "http://"):
        if host.startswith(prefix):
            host = host[len(prefix) :]
    config = databricks_config()
    return sql.connect(
        server_hostname=host.rstrip("/"),
        http_path=env["DATABRICKS_HTTP_PATH"],
        credentials_provider=lambda: config.authenticate,
    )


def ensure_raw_table(cursor: Any) -> None:
    """Create the schema and the raw table if they do not exist yet."""
    fqn = raw_table_fqn()
    catalog, schema, _ = fqn.split(".")
    cursor.execute(f"create schema if not exists {catalog}.{schema}")
    cursor.execute(_CREATE_TABLE_SQL.format(fqn=fqn))
    LOGGER.info("Ensured raw table %s", fqn)


def _row_chars(row: Sequence[Any]) -> int:
    """Approximate the parameter cost of one row, in characters.

    Strings dominate and are what the server counts; the remaining columns are a
    source id, a URL and timestamps, which contribute a few bytes each.
    """
    return sum(len(value) if isinstance(value, str) else 8 for value in row)


def chunk_rows(rows: Sequence[Sequence[Any]]) -> list[list[Sequence[Any]]]:
    """Split rows into chunks that respect both the row and the parameter budget."""
    chunks: list[list[Sequence[Any]]] = []
    current: list[Sequence[Any]] = []
    size = 0
    for row in rows:
        cost = _row_chars(row)
        if current and (len(current) >= INSERT_CHUNK_ROWS or size + cost > INSERT_CHUNK_CHARS):
            chunks.append(current)
            current, size = [], 0
        current.append(row)
        size += cost
    if current:
        chunks.append(current)
    return chunks


def insert_postings(cursor: Any, postings: Iterable[dict[str, Any]]) -> int:
    """Append the normalised rows to the raw table, stamping ingest time.

    Returns the number of rows written. Values are bound as ``?`` parameters;
    only the table name is interpolated.
    """
    ingested_at = utcnow()
    rows = [tuple([posting.get(column) for column in RAW_COLUMNS[:-1]] + [ingested_at]) for posting in postings]
    if not rows:
        LOGGER.info("Nothing to insert; raw table left untouched")
        return 0

    fqn = raw_table_fqn()
    written = 0
    for chunk in chunk_rows(rows):
        # One statement carrying many value tuples; parameters stay bound.
        statement = _INSERT_PREFIX.format(fqn=fqn) + ", ".join([_ROW_PLACEHOLDER] * len(chunk))
        cursor.execute(statement, tuple(value for row in chunk for value in row))
        written += len(chunk)
        LOGGER.info("Inserted %d/%d postings", written, len(rows))
    return written
