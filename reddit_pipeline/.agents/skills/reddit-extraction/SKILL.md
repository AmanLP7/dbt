---
name: reddit-extraction
description: Running, debugging, or smoke-testing the three Reddit extraction modules, the shared `_common.py` scaffolding they rely on, the Dagster code location that orchestrates them (three extractor assets merged into the dbt sources, then one `dbt build` asset) — what each loads into Databricks, the function entrypoints and the raise-not-sys.exit rule retries depend on, the required .env variables, the rolling 5-hour window that makes the posts/comments loads incremental, the key-based upsert for users, the single parameterised insert path, the trailing `ingested_at` column, the missing runtime dependencies, and how to exercise the modules end to end without network or credentials using stubbed clients.
globs: ["ingestion/reddit/**/*.py", "orchestration/**/*.py", "tests/**/*.py"]
---

# Reddit extraction scripts

## Entrypoints and orchestration

Each extractor is an importable module exposing a single function entrypoint:

| Module | Entrypoint | Returns |
| --- | --- | --- |
| `ingestion/reddit/extract_reddit.py` | `extract_posts()` | post rows written |
| `ingestion/reddit/extract_reddit_comments.py` | `extract_comments()` | comment rows written |
| `ingestion/reddit/extract_reddit_users.py` | `extract_users()` | user rows written |

Importing a module loads `.env` and nothing else; all Reddit API and Databricks
work happens inside the entrypoint. Each module is runnable directly, where
`__main__` calls `configure_logging()` and then its entrypoint, so both paths
behave the same:

```bash
python -m ingestion.reddit.extract_reddit
```

The `-m` form (`ingestion/reddit/_common.py` and the extractors use absolute
imports) must be run from the repo root.

`orchestration/src/reddit_pipeline_orchestration/definitions.py` imports the three
and wraps each in a Dagster asset — `reddit_raw/r_worldnews`,
`reddit_raw/r_comments`, `reddit_raw/r_users` — plus one `@dbt_assets` definition,
`dbt_reddit`. Comments and users declare `deps=[RAW_POSTS_KEY]`, so the job runs
posts -> comments/users -> dbt (comments and users derive from the same hot posts,
so the order matters). Each extractor asset is declared under the asset key Dagster
derives for its `reddit_raw.r_*` source, so the source spec merges into the extractor
and the staging models depend on the loads. `dbt_reddit` runs
`dbt build --profiles-dir profiles` from the repo root through `DbtCliResource`; with
no `--select` that builds and tests every model in the project, so the step works on a
warehouse where dbt has never run. The dbt models declare their own dependencies from
the manifest, so they are not hand-listed in the job. `reddit_refresh` is a job over
`AssetSelection.all()` with **no schedule** — runs are on demand, from the UI or
`dagster asset materialize`. All four assets carry three retries with an exponential
backoff plus jitter, ported from the Prefect tasks. See
`orchestration/README.md` for the full graph, commands and deployment notes.

**Credentials are shared.** The dbt asset does not rely on dbt's own auth:
`refresh_dbt_token()` mints a token with
`databricks.sdk.core.Config(profile=…).authenticate()` from the same Databricks CLI
profile the extractors use and exports it as `DATABRICKS_TOKEN` immediately before
`dbt` runs (the token is also refreshed when the code location loads, so a
long-lived server does not serve an expired one). One `databricks auth login` covers
both halves, and the two cannot disagree about the workspace. Passing a token is
deliberate: dbt-databricks' own `auth_type: oauth` is a browser U2M flow that cannot
complete unattended. `profiles/profiles.yml` is the repo-owned profile the resource
points dbt at (`--profiles-dir profiles`). If the CLI profile is unavailable, the
refresh logs a warning and leaves an existing `DATABRICKS_TOKEN` in place — that is
the CI path.

The entrypoints `raise` on failure and **never call `sys.exit(1)`**. Dagster records
a raised exception as a failed materialization, the only state the retry policy acts
on; `sys.exit` ends the worker process instead. Keep that rule when editing these
scripts.

## Shared scaffolding (`ingestion/reddit/_common.py`)

Everything the three extractors used to duplicate lives here:

- constants: `INCREMENTAL_WINDOW_HOURS = 5`, `SUBREDDIT = "worldnews"`,
  `CATALOG`/`SCHEMA`, `POSTS_LIMIT = 1000`, `COMMENTS_PER_POST = 10`,
  `USER_COMMENTS_PER_POST = 15`, `TARGET_USER_COUNT = 200`, `REQUIRED_ENV`, and
  the per-table column mappings (`POSTS_COLUMNS`, `COMMENTS_COLUMNS`,
  `USERS_COLUMNS`).
- helpers: `validate_env()`, `reddit_client()`, `connect()`, `full_table()`,
  `window_cutoff()`, `load_timestamp()`, `ensure_table()`, `ensure_columns()`,
  `replace_window()`, `replace_by_key()`, `configure_logging()`, plus a module
  `logger`.

The extractors import these by name; they own no I/O of their own (no `praw`, no
`sql` call outside `_common`). Public entrypoints are unchanged: each returns
`int`, the number of rows written.

## What each script loads

| Module | Source | Target table | Volume |
| --- | --- | --- | --- |
| `ingestion/reddit/extract_reddit.py` | `subreddit.hot(limit=1000)` | `workspace.topic_raw.r_worldnews` | 1 row/post |
| `ingestion/reddit/extract_reddit_comments.py` | same hot posts, `comments[:10]` each, `replace_more(limit=0)` | `workspace.topic_raw.r_comments` | ~10000 rows |
| `ingestion/reddit/extract_reddit_users.py` | post authors and comment authors from `subreddit.hot(limit=1000)`, then `subreddit.new(limit=1000)` fallback | `workspace.topic_raw.r_users` | ≤ 200 profiles |

Each script calls `ensure_table()` (which issues `CREATE CATALOG/SCHEMA/TABLE IF
NOT EXISTS` before loading) and then `ensure_columns()`.

Loads are incremental, not full refreshes:

- **Posts and comments** load a **rolling 5-hour window** on the entity's own
  `created_utc` (`INCREMENTAL_WINDOW_HOURS = 5`): `window_cutoff()` computes
  `time.time() - 5 * 3600` once per run, then `replace_window()` binds that cutoff
  into `DELETE FROM <table> WHERE created_utc >= ?` and inserts only the fetched
  rows at or after it, so the entrypoint returns the in-window row count rather
  than the fetched count. Rows older than the window are retained and never
  rewritten: both tables accumulate history. If no fetched row falls in the window
  the run skips **both** the delete and the insert — it never deletes with nothing
  to insert, and it logs how many rows were fetched versus kept in-window.
- **Users** have no window: an account's `created_utc` is its account age, not
  recency. `replace_by_key()` is a key-based upsert — `DELETE FROM ... WHERE id IN
  (?, ?, ...)` (ids passed as bound parameters) followed by an insert of all
  fetched rows — so returning users get refreshed karma/gold and new users are
  added, without duplicates. Nothing fetched means both statements are skipped.
  Profile reads are what trigger the per-author API request, so a suspended,
  deleted or private account raises there; the collector counts those failures,
  logs each at `WARNING`, and reports the total in its summary instead of
  swallowing them.

Two consequences, stated honestly:

- The delete is scoped to the window and the source fetch is bounded, so a post
  or comment that ages out of the source while still inside the window is
  deleted and not re-inserted unless the fetch still returns it.
- The DELETE and INSERT are separate autocommitted statements. A failure between
  them leaves the window rows absent rather than restoring the previous
  snapshot; a retry re-fetches and rewrites the window.

This still bounds every downstream number: `r_comments` only ever holds the
comments the extractor actually saw — those of the posts hot in the rolling
window — so `mart_post_summary.comments_collected_locally` reflects the
extraction window, not the post's true comment count (`total_comments_on_reddit`).

## One parameterised insert path

All three loaders now share a single strategy: `_insert_sql()` builds
`INSERT INTO <table> (<cols>) VALUES (?, ?, ...)` and `executemany` binds one
tuple per row. Nothing about a row is interpolated into SQL text — the two
hand-rolled `escape_str()` helpers that used to build `INSERT`/`WHERE id IN`
strings for the posts and users loads are gone. The only things ever interpolated
are identifiers (catalog, schema, table and column names), which cannot be bound.

`replace_window()` and `replace_by_key()` differ only in the delete predicate
(window cutoff vs. fetched key set); both then call `_insert_sql()` +
`executemany`. Tests assert the payload never reaches the SQL string: a comment
body containing `O'Brien; DROP TABLE …; --` travels as a parameter
(`tests/test_extractors.py`).

## Raw table columns and `ingested_at`

The column mappings in `_common.py` are the source of truth; `ingested_at
TIMESTAMP` is the **trailing** column of all three tables and holds the UTC time
the row was loaded (`load_timestamp()`, timezone-aware), not a value Reddit
supplied. One timestamp is computed per run and stamped on every row.

`ensure_columns()` additively migrates a table that predates the column: it runs
`DESCRIBE TABLE`, then one `ALTER TABLE ... ADD COLUMNS (name type)` per missing
column, and is a no-op when nothing is missing. This is how an already-shipped
table gains `ingested_at` without a drop-and-recreate.

## Environment

`.env` at the project root supplies the required keys `REDDIT_CLIENT_ID`,
`REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`, `DATABRICKS_HOSTNAME`,
`DATABRICKS_HTTP_PATH`; the optional `DATABRICKS_CONFIG_PROFILE` (default
`DEFAULT`) and `LOG_LEVEL` (default `INFO`). `.env.example` is the template and
`.env` is gitignored — never commit it, never print its values. `validate_env()`
runs first in every entrypoint and raises if any required value is missing or
still contains the placeholder `your_`.

Databricks credentials do **not** come from `.env`. `connect()` builds
`Config(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))` and passes
`credentials_provider=lambda: databricks_config.authenticate` to `sql.connect()`.
The profile lives in `~/.databrickscfg`; create it with
`databricks auth login --host <workspace-url>`, which stores an OAuth token the
SDK refreshes. Notes:

- The value must be a **factory**: the connector does
  `_header_factory = credentials_provider()` and then calls `_header_factory()`
  per request (`databricks/sql/auth/authenticators.py`), so passing
  `credentials_provider=databricks_config.authenticate` directly raises
  `RequestError: 'dict' object is not callable`.
- `databricks-sdk` provides `Config`; the same library gives the dbt asset its token.
- The profile's host must match `DATABRICKS_HOSTNAME`; the OAuth token is scoped
  to one workspace.

Runtime deps (pinned in `requirements.txt`): `praw`, `databricks-sql-connector`
(provides `databricks.sql`), `databricks-sdk` (provides `databricks.sdk`),
`python-dotenv`, `dbt-databricks` (provides the `dbt` CLI the dbt asset invokes;
nothing imports it, so it does not affect `pyright`), and the orchestration pair
`dagster`/`dagster-dbt` plus `dagster-webserver` for `dagster dev`.
`pip install -r requirements.txt -e orchestration` puts them in the active
environment, after which `pyright` stops reporting missing imports.

## Logging

Logging is the stdlib `logging` module — there is no `print` left in the
extractors. `_common.logger` is the module logger; per-post progress is emitted at
`DEBUG`, while a normal run reports per-phase summaries at `INFO` (posts fetched
vs. kept in-window, users collected, profile-fetch failures, rows written).
`LOG_LEVEL` (default `INFO`) controls the root level through
`configure_logging()`, which is called only under `if __name__ == "__main__":` in
each module — so importing a module installs no handler, and tests can assert on
`caplog` records.

## Smoke test without network or credentials

`ingestion/reddit/_common.py` imports its runtimes (`praw`, `databricks.sql`,
`databricks.sdk.core`) at module scope but does no network or DB work on import,
and the extractors touch no I/O directly — so patching `_common` alone is enough.
Stub the four patch points, then call the entrypoint; this exercises the real
control flow and lets you capture the SQL actually issued:

```python
import os
import types

from ingestion.reddit import _common
from ingestion.reddit.extract_reddit import extract_posts

for var in _common.REQUIRED_ENV:
    os.environ[var] = f"test-{var.lower()}"

_common.praw.Reddit = lambda **_: FakeReddit()
_common.sql.connect = lambda **_: FakeConnection()
_common.time = types.SimpleNamespace(time=lambda: 1_700_000_000.0)
_common.Config = lambda **_: types.SimpleNamespace(authenticate=lambda: {"Authorization": "Bearer stub"})

extract_posts()
```

- `ingestion.reddit._common.praw.Reddit` — the Reddit client factory.
- `ingestion.reddit._common.sql.connect` — the Databricks connection; return a
  fake connection whose cursor records `execute`/`executemany`.
- `ingestion.reddit._common.time` — pin the clock, keeping the real `time` module
  untouched (this is what makes the window cutoff deterministic).
- `ingestion.reddit._common.Config` — the CLI-profile credential object, so
  `authenticate()` returns a stub header dict.

Set the env vars first: the entrypoint validates them as its first step. Fake
`Submission`/`Comment`/`Author` objects are enough; assert on captured column
names, tuple widths, row counts, and the bound delete parameters.
`databricks.sdk.core.Config` is imported at module scope, so stub it unless
`databricks-sdk` is installed — the captured `sql.connect()` kwargs carry the
`credentials_provider` factory.

The maintained version of this recipe is the offline suite in `tests/`:
`conftest.py` wires the fakes into exactly these patch points, stubs
`dotenv.load_dotenv` before importing the project modules (so a developer's real
`.env` cannot leak in) and defaults the dummy `DATABRICKS_*` values the code
location needs for `dbt parse`; `fakes.py` holds the doubles, and
`test_extractors.py`, `test_definitions.py`, `test_extract_assets.py` and
`test_window_contract.py` assert the behaviour. Run it with `pytest`.

## Debugging

```bash
python -m debugpy --listen 5678 --wait-for-client -m ingestion.reddit.extract_reddit
```

`debugpy` is pinned in `requirements-dev.txt`. `validate_env()`'s `bad_vars`
comprehension uses `not (value := os.getenv(var)) or "your_" in value`, which
narrows `value` to `str`, so pyright reports nothing there — see the
`lint-and-typecheck` skill.
