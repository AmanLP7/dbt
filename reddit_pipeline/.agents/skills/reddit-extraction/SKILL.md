---
name: reddit-extraction
description: Running, debugging, or smoke-testing the three Reddit extraction modules, the shared `_common.py` scaffolding they rely on, the `copy_into.py` loader that lands their SQLite dump in Databricks, and the Dagster code location that orchestrates them (extractor assets merged into the dbt sources, three raw assets that each dump then COPY INTO their own table, then one `dbt build` asset) — what each dumps into the local SQLite run database and how `COPY INTO` lands it, the function entrypoints and the raise-not-sys.exit rule retries depend on, the required .env variables, the seed-driven subreddit list, the 26-hour read window, the SQLite dump and append-only `COPY INTO` load all three share, the trailing `ingested_at` column, the missing runtime dependencies, and how to exercise the modules end to end without network or credentials using stubbed clients.
globs: ["ingestion/reddit/**/*.py", "orchestration/**/*.py", "tests/**/*.py"]
---

# Reddit extraction scripts

## Entrypoints and orchestration

Each extractor is an importable module exposing a single function entrypoint:

| Module | Entrypoint | Returns |
| --- | --- | --- |
| `ingestion/reddit/extract_reddit.py` | `extract_posts()` | post rows dumped to SQLite |
| `ingestion/reddit/extract_reddit_comments.py` | `extract_comments()` | comment rows dumped to SQLite |
| `ingestion/reddit/extract_reddit_users.py` | `extract_users()` | user rows dumped to SQLite |

Importing a module loads `.env` and nothing else; all Reddit API and SQLite
work happens inside the entrypoint. Each module is runnable directly, where
`__main__` calls `configure_logging()` and then its entrypoint, so both paths
behave the same:

```bash
python -m ingestion.reddit.extract_reddit
```

The `-m` form (`ingestion/reddit/_common.py` and the extractors use absolute
imports) must be run from the repo root.

`orchestration/src/reddit_pipeline_orchestration/definitions.py` imports the three
and wraps each in a Dagster asset — `reddit_raw/r_posts`,
`reddit_raw/r_comments`, `reddit_raw/r_users` — each of which dumps its rows and then
COPY INTOs its own table, plus one `@dbt_assets` definition, `dbt_reddit`. Comments
and users declare `deps=[RAW_POSTS_KEY]`, so the job runs
posts -> comments/users -> dbt (comments and users derive from the same
post listings, so the order matters). Each extractor asset is declared under the asset
key Dagster derives for its `reddit_raw.r_*` source, so the source spec merges into
the extractor and the staging models depend on the loads. `dbt_reddit` runs
`dbt build --profiles-dir profiles` from the repo root through `DbtCliResource`; with
no `--select` that builds and tests every model in the project, so the step works on a
warehouse where dbt has never run. The dbt models declare their own dependencies from
the manifest, so they are not hand-listed in the job. `reddit_refresh` is a job over
`AssetSelection.all()`; its one schedule is `daily_edition_refresh` (cron
`0 5 * * *`, UTC), which ships **stopped**, so nothing writes on its own until it is
turned on in the UI. The three extractor assets and `dbt_reddit` carry three
retries with an exponential backoff plus jitter, ported from the Prefect tasks. See
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

- constants: `INCREMENTAL_WINDOW_HOURS = 26`, `SECTION_SEED`, `CATALOG`/`SCHEMA`,
  the two listing bounds (`NEW_POSTS_LIMIT`, `TOP_POSTS_LIMIT`, `TOP_TIME_FILTER`),
  the comment and user budgets (`COMMENT_POST_LIMIT`, `COMMENTS_PER_POST`,
  `USER_POST_LIMIT`, `USER_COMMENTS_PER_POST`, `TARGET_USER_COUNT`),
  `REQUIRED_ENV`, and the per-table column mappings (`POSTS_COLUMNS`,
  `COMMENTS_COLUMNS`, `USERS_COLUMNS`).
- helpers: `validate_env()`, `reddit_client()`, `connect()`, `full_table()`,
  `subreddit_list()`, `subreddit_listings()`, `harvest_submissions()`,
  `row_values()`, `window_cutoff()`, `load_timestamp()`, `ensure_table()`,
  `ensure_columns()`, `configure_logging()`, plus the SQLite dump path
  (`connect_sqlite()`, `ensure_sqlite_table()`, `dump_rows()`) and the COPY INTO
  machinery (`copy_selection()`, `copy_into_statement()`, `export_ndjson()`,
  `VolumeFiles`), plus a module `logger`.

The extractors import these by name; they own no I/O of their own (no `praw`, no
`sql` call outside `_common`). Public entrypoints are unchanged: each returns
`int`, the number of rows dumped to SQLite. `copy_into.py` is the second half of the
load and lives beside the extractors.

## What each script loads

| Module | Source | SQLite table → raw table | Volume |
| --- | --- | --- | --- |
| `ingestion/reddit/extract_reddit.py` | every seeded subreddit: `new(limit=100)` + `top(limit=100, time_filter='day')` | `r_posts` → `workspace.topic_raw.r_posts` | 1 row/post, ≤ 13,200 posts |
| `ingestion/reddit/extract_reddit_comments.py` | the 200 most-commented in-window posts, `comments[:10]` each, `replace_more(limit=0)` | `r_comments` → `workspace.topic_raw.r_comments` | ≤ 2,000 rows |
| `ingestion/reddit/extract_reddit_users.py` | post authors from `new(limit=50)` across the seed, then comment authors if short | `r_users` → `workspace.topic_raw.r_users` | ≤ 200 profiles |

**The subreddit list is the seed.** `subreddit_list()` reads
`seeds/sections.csv`, the same file that maps a subreddit to its newspaper section,
so adding a subreddit is one seed row and the ingest cannot drift from the marts'
section map. 66 subreddits today; two listing requests each, so 132 requests per
posts run against a 6,000/hour budget. A subreddit that cannot be read — private,
banned, renamed, rate limited — is logged at `WARNING` and skipped rather than
failing a run that has already spent requests on the others.

Posts are deduped by id across both listings before dumping, so a story that is both
new and top-of-day is written once, with the `top` reading's score (that listing runs
second and carries the fresher number). Comments are fetched only for the most
commented posts because comment fetching is one request per post.

Each extractor calls `ensure_sqlite_table()` to create its SQLite table (mapping the
Databricks column types onto SQLite: `STRING`→`TEXT`, `INT`→`INTEGER`, `DOUBLE`→
`REAL`, `BOOLEAN`→`INTEGER`, `TIMESTAMP`→`TEXT`) before dumping. `copy_into.py` then
calls `ensure_table()` (which issues `CREATE CATALOG/SCHEMA/TABLE IF NOT EXISTS`) and
`ensure_columns()` on the Databricks target before each `COPY INTO`.

Loads are a SQLite dump followed by an append-only `COPY INTO`, all three of them:

- Each extractor `dump_rows()`es its fetched rows into its SQLite table. A hand-run
  replaces the table first (`replace=True`); the orchestrated run uses one fresh
  `data/reddit.db` per run. Nothing is ever deleted from the warehouse: `COPY INTO`
  appends, so the raw tables accumulate history and the dbt staging models already
  dedupe by id (`row_number() over (partition by id order by created_utc desc, score
  desc)`), which is why no upsert or DELETE is needed.
  A time-scoped delete cannot be exact once a run reads many subreddits through
  bounded listings: two consecutive runs cover different spans of the same day, so
  the second would delete rows the first wrote and never refetch them. Appending
  sidesteps that entirely.
- `INCREMENTAL_WINDOW_HOURS = 26` survives as a **read** filter only:
  `window_cutoff()` computes `time.time() - 26 * 3600` once per run, and fetched
  rows older than it are dropped before the dump. The entrypoint returns the
  written count, and logs fetched-versus-kept. 26 rather than 24 because a daily
  cadence with a 24-hour window leaves a gap whenever the run lands a minute late.
- **Users** carry no window at all: an account's `created_utc` is its account age,
  not recency. Profile reads are what trigger the per-author API request, so a
  suspended, deleted or private account raises there; the collector counts those
  failures, logs each at `WARNING`, and reports the total in its summary instead of
  swallowing them.

Two consequences, stated honestly:

- `COPY INTO` is append-only and runs only after the whole dump has been staged on
  the volume, so a failed load never removes a row that was already there. If a
  retry lands the same files again, the staging models dedupe by id; if a later run
  re-fetches the same ids, the older rows stay and the newer ones win in staging.
- A post that ages out of both listings simply stops being refreshed. Its row stays,
  so the raw tables accumulate history — nothing prunes them, and `open_listings`-style
  liveness is not what they measure.

This still bounds every downstream number: `r_comments` only ever holds the
comments the extractor actually saw — those of the posts hot in the rolling
window — so `mart_post_summary.comments_collected_locally` reflects the
extraction window, not the post's true comment count (`total_comments_on_reddit`).

## One SQLite dump path

All three loaders share a single strategy: `dump_rows()` builds
`INSERT INTO <table> (<cols>) VALUES (?, ?, ...)` once and the rows travel in one
`executemany` with `?` parameters. Nothing about a row is interpolated into SQL text
— the two hand-rolled `escape_str()` helpers that used to build `INSERT`/`WHERE id
IN` strings for the posts and users loads are gone. The only things ever
interpolated are identifiers (catalog, schema, table and column names), which cannot
be bound. SQLite is a local file, so there is no per-row network round trip to
measure and no multi-row batch bound: `executemany` is exactly right here.

`copy_into.py` then exports the dump as NDJSON — `export_ndjson()` streams each SQLite
table into `part-00000.json` files, splitting when a file reaches
`NDJSON_FILE_BYTES` (~200 MB), and `VolumeFiles` uploads them to the staging volume.
One `COPY INTO` per table reads those files back out, with `copy_selection()` casting
each column to its Databricks type (`ingested_at` back to `TIMESTAMP`) so the raw
tables land typed. Tests assert the payload never reaches the SQL string: a comment
body containing `O'Brien; DROP TABLE …; --` travels as a bound parameter
(`tests/test_extractors.py`).

## Raw table columns and `ingested_at`

The column mappings in `_common.py` are the source of truth; `ingested_at
TIMESTAMP` is the **trailing** column of all three tables and holds the UTC time
the row was loaded (`load_timestamp()`, timezone-aware), not a value Reddit
supplied. One timestamp is computed per run and stamped on every row.

`ensure_columns()` additively migrates a table that predates the column: it runs
`DESCRIBE TABLE`, then one `ALTER TABLE ... ADD COLUMNS (name type)` per missing
column, and is a no-op when nothing is missing. This is how an already-shipped
table gains `ingested_at` without a drop-and-recreate. On the way in, the SQLite
dump stores `ingested_at` as `TEXT` (UTC string); the `COPY INTO` SELECT casts it
back to `TIMESTAMP` (`copy_selection()`), which is what keeps `src_reddit.yml`'s
`loaded_at_field: ingested_at` freshness working.

## Environment

`.env` at the project root supplies the required keys `REDDIT_CLIENT_ID`,
`REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`, `DATABRICKS_HOSTNAME`,
`DATABRICKS_HTTP_PATH`; the optional `DATABRICKS_CONFIG_PROFILE` (default
`DEFAULT`) and `LOG_LEVEL` (default `INFO`), plus the optional
`REDDIT_SQLITE_PATH` (default `data/reddit.db`) and `REDDIT_VOLUME` (default
`workspace.topic_raw.staging`). `.env.example` is the template and
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
control flow and lets you capture the rows dumped to SQLite:

```python
import os
import sqlite3
import types

from ingestion.reddit import _common
from ingestion.reddit.extract_reddit import extract_posts

for var in _common.REQUIRED_ENV:
    os.environ[var] = f"test-{var.lower()}"

_common.praw.Reddit = lambda **_: FakeReddit()
_common.connect_sqlite = lambda *a, **k: sqlite3.connect(":memory:")
_common.time = types.SimpleNamespace(time=lambda: 1_700_000_000.0)
_common.Config = lambda **_: types.SimpleNamespace(authenticate=lambda: {"Authorization": "Bearer stub"})

extract_posts()
```

- `ingestion.reddit._common.praw.Reddit` — the Reddit client factory.
- `ingestion.reddit._common.connect_sqlite` — return an in-memory (or temp-file)
  SQLite connection; the entrypoint opens its own connection through it, so query
  `r_posts` afterwards to assert the dumped rows.
- `ingestion.reddit._common.time` — pin the clock, keeping the real `time` module
  untouched (this is what makes the window cutoff deterministic).
- `ingestion.reddit._common.Config` — the CLI-profile credential object, so
  `authenticate()` returns a stub header dict.

Set the env vars first: the entrypoint validates them as its first step. Fake
`Submission`/`Comment`/`Author` objects are enough; assert on captured column
names, tuple widths, and the row counts/values in the SQLite table. To exercise
the load half without a warehouse, call `copy_into_raw(dry_run=True)` (counts the
SQLite rows, touches nothing on Databricks) or pass `volume_files=FakeVolumeFiles()`
to capture the uploads while faking `_common.connect()` and `_common.VolumeFiles`.
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
