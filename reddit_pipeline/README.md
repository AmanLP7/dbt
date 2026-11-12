# Reddit Data Pipeline

This dbt project is designed to transform and model Reddit data on Databricks.

## Architecture & Flow
1. **Extraction**: Fetch every subreddit named in `seeds/sections.csv` (66 today) with PRAW — each one's `new` and `top(day)` listings, the most-commented posts' comments, and the authors' profiles.
2. **Load**: Upsert into Databricks Delta by id — a chunked `DELETE WHERE id IN (…)` then a batched insert. All three loaders work this way; a 26-hour read filter decides which fetched rows are worth writing, and nothing is ever deleted by time.
3. **Transformation (dbt)**: Model and clean raw data into staging, intermediate, and marts layers.
4. **Orchestration**: a Dagster job runs the extractors in order, then one `dbt build`.

## Project Structure
- `models/staging/`: Raw source data cleaning, renaming, and type casting.
- `models/intermediate/`: Complex joins, aggregations, and business logic.
- `models/marts/`: Final analytics-ready tables for consumption. `mart_post_summary` and `mart_user_metrics` are rebuilt on every run; the comment-id-grain `mart_comment_history` (which combines `r_comments` with its `r_posts` post context over a 26-hour window) is `incremental` and merges on `comment_id`, so history accumulates instead of being recomputed.
- `models/marts/reddit/daily/`: the daily edition. `mart_daily_edition` (ranked stories per section), `mart_daily_section_summary`, `mart_daily_trending_terms`, `mart_daily_reading_list`, `mart_subreddit_daily_volume`. All five are `incremental` on `edition_date` with `delete+insert`, so each run replaces the last three editions and every older edition survives the raw tables rolling. The edition date is the story's own creation date, not the build date, so rebuilding reproduces the same paper.
- Each layer keeps its models in a source-named subfolder — currently `reddit/`, so a second source can sit beside it: `models/staging/reddit/`, `models/intermediate/reddit/`, `models/marts/reddit/`. The layer's materialisation in `dbt_project.yml` is inherited by the subfolder.
- `ingestion/reddit/`: the Python extractors that fetch Reddit data and load it into Databricks — one module per entity, plus `_common.py` holding the scaffolding they share (constants, env validation, clients, table setup, the key-based upsert). Like the model layers, `ingestion/` groups extractors into a source-named subpackage — currently `reddit/`; `ingestion/__init__.py` and `ingestion/reddit/__init__.py` are package markers.
- `orchestration/`: the Dagster code location (`reddit_pipeline_orchestration.definitions`). Three extractor assets and one dbt-assets definition, merged into a single graph by asset key; see [Orchestration](#orchestration).
- `tests/`: the offline pytest suite — `conftest.py` fixtures, `fakes.py` test doubles, and the extractor/definitions/asset/window-contract tests. No network and no warehouse.
- `profiles/profiles.yml`: the repo-owned dbt profile (see Quick Start).
- `seeds/sections.csv`: maps a subreddit to its newspaper section (`world`, `politics`, `business`, `tech`, `science`, `sports`, `culture`, `discussion`) plus a weight. A subreddit absent from the seed still prints, under `other`; the seed decides the front page's shape, not whether a subreddit is ingested.
- `seeds/trending_stopwords.csv`: words removed from story titles before the trending-terms mart counts them — grammar and news furniture only.
- `macros/`: Reusable Jinja macros, e.g. `add_audit_fields()`, `window_hours()` and `story_domain()`.
- `.env.example`: the configuration template; copy it to `.env` (gitignored) and fill it in.
- `dbt_project.yml`: project config — `model-paths: ["models"]`, `macro-paths: ["macros"]`, the standard `analysis-paths`/`test-paths`/`seed-paths`/`snapshot-paths` entries and `clean-targets`. Materialisation is set per layer, and the rolling-window constant lives in the `window_hours()` macro rather than as a dbt variable.
- `tests/` is both dbt's default singular-test directory and the pytest suite's directory. That overlap is harmless: dbt only collects `.sql` files from `test-paths`, so the Python suite is invisible to it. `analyses/` and `snapshots/` are the dbt defaults and hold only `.gitkeep`; `seeds/` carries the two CSVs above.

## Quick Start

### Prerequisites
- Python 3.10+
- The pipeline requirements, which include `dbt-databricks` and therefore the
  `dbt` CLI, plus the orchestration package:
  ```bash
  pip install -r requirements.txt -e orchestration
  ```
- Configuration copied from the template:
  ```bash
  cp .env.example .env
  ```
  `.env` is gitignored. Any required value left at a `your_` placeholder is
  rejected at startup, so a half-configured copy fails loudly. See
  [Credentials](#credentials) for the keys.

### Profile Setup
The repo carries its own dbt profile at `profiles/profiles.yml`, so there is
nothing to create in `~/.dbt`. Its `host`, `http_path` and `token` all come from
`env_var(...)`, and the Dagster dbt resource points dbt at it with `--profiles-dir
profiles`.
The token is derived from the **same Databricks CLI profile the extractors use**,
so one `databricks auth login` covers both ingestion and the mart build:
```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
```
`DATABRICKS_CONFIG_PROFILE` selects a non-`DEFAULT` CLI profile. See
[Orchestration](#orchestration) for how the orchestrated run injects the token.

A hand-run `dbt` from the repo root still resolves `~/.dbt/profiles.yml`, whose
PAT may be invalid. Point manual runs at the repo profile and export the same
values the orchestrated run would:
```bash
export DATABRICKS_HOSTNAME=... DATABRICKS_HTTP_PATH=... DATABRICKS_TOKEN=...
DBT_PROFILES_DIR=profiles dbt build
```

### Running the Pipeline
Check connection (needs the env vars above exported, since `profiles/` reads
them):
```bash
DBT_PROFILES_DIR=profiles dbt debug
```

Run models:
```bash
DBT_PROFILES_DIR=profiles dbt run
```

Run tests:
```bash
DBT_PROFILES_DIR=profiles dbt test
```

`dbt build` runs the models and their tests together in dependency order.

## Credentials

The extractors and the orchestrator read a single `.env` at the project root
(`load_dotenv()`), never a committed file. `.env.example` is the template:

| Key | Required | Purpose |
| --- | --- | --- |
| `REDDIT_CLIENT_ID` | yes | Reddit API application id |
| `REDDIT_CLIENT_SECRET` | yes | Reddit API application secret |
| `REDDIT_USER_AGENT` | yes | User agent string PRAW requires |
| `DATABRICKS_HOSTNAME` | yes | Workspace host the SQL warehouse lives on |
| `DATABRICKS_HTTP_PATH` | yes | HTTP path of the SQL warehouse |
| `DATABRICKS_CONFIG_PROFILE` | no | CLI profile in `~/.databrickscfg`; default `DEFAULT` |
| `LOG_LEVEL` | no | Extractor log level; default `INFO` |

Databricks credentials do **not** come from a PAT in `.env`. The extractors
authenticate with the OAuth token stored by `databricks auth login` in the CLI
profile; the profile's host must be the same workspace as `DATABRICKS_HOSTNAME`.
The `reddit-extraction` skill documents the client construction and the offline
test recipe.

## Orchestration

Runs are orchestrated with Dagster. Install the pipeline runtime dependencies and
the orchestration package — all **into the active Python environment**:
```bash
pip install -r requirements.txt -e orchestration
```

The package installs editable so the `dagster` CLI, the code location, the
extractors and the `dbt` CLI resolve from one interpreter: the code location imports
the extractors and drives dbt through `DbtCliResource`.

`dagster dev` serves the UI, scheduler and daemon; the first run writes its history to
`$DAGSTER_HOME` (default `~/.dagster`). The full orchestration reference — the asset
graph, groups, checks, commands and the token hand-off — is
[`orchestration/README.md`](orchestration/README.md).

The extraction scripts need `REDDIT_*` and `DATABRICKS_HOSTNAME`/`DATABRICKS_HTTP_PATH`
in `.env` (gitignored); see [Credentials](#credentials) for the full key list and
the `reddit-extraction` skill for what each script loads and how to exercise them
offline with stubbed clients.

They authenticate to Databricks with the **CLI profile** in `~/.databrickscfg`, not
with a PAT: log in once, and the OAuth token is refreshed for you.
```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
```
`DATABRICKS_CONFIG_PROFILE` selects a non-`DEFAULT` profile. The profile's host
must be the same workspace as `DATABRICKS_HOSTNAME`.

### Extraction and dbt build

The three extractors are importable modules in the `ingestion.reddit` package.
`ingestion/reddit/extract_reddit.py`, `ingestion/reddit/extract_reddit_comments.py`
and `ingestion/reddit/extract_reddit_users.py` each expose a function entrypoint —
`extract_posts()`, `extract_comments()`, `extract_users()` — that returns the
number of rows written; importing a module runs no extraction. Each module is
still runnable directly, where `__main__` calls its entrypoint and installs the
logging config:
```bash
python -m ingestion.reddit.extract_reddit
```
They live under `ingestion/` rather than under `models/` because dbt treats every
`.py` file under its model paths as a Python model. Everything the three used to
duplicate now lives in `ingestion/reddit/_common.py` — the tuning constants, env
validation, the Reddit and Databricks clients, table setup, and the key-based load
strategy described below.

Loads are incremental, not full refreshes, and each writes a trailing
`ingested_at TIMESTAMP` column stamped at load time (the UTC time the row was
written, not a value Reddit supplied):

All three loaders are **key-based upserts**: `DELETE FROM <table> WHERE id IN
(<fetched ids>)` in chunks of 5,000, each chunk's delete immediately followed by that
chunk's batched insert. A rolling
window is not used, because a run reads several subreddits through bounded listings
and two consecutive runs cover different spans of the same day — a
`WHERE created_utc >= <cutoff>` delete would drop rows the run never refetched. The
window survives only as a *read* filter on the posts and comments loads:
`INCREMENTAL_WINDOW_HOURS = 26` decides which fetched rows are worth writing, and
nothing is ever deleted by time. The users load carries no window at all — it upserts
whatever profiles the run collected.

26 rather than 24 because the pipeline runs once a day and a 24-hour window landing
a minute late leaves a gap between consecutive runs. Posts and comments upsert on
`id`; users upsert on the profile `id`, so a returning user gets refreshed
karma/gold and a new one is added. Per-author profile failures (suspended, deleted,
private) are counted, logged at `WARNING`, and reported in the run summary rather
than swallowed. A post fetched with nothing new inside the window skips both the
delete and the insert.

`stg_reddit_*` still dedupes by id. That is the at-least-once case, not the
expected one: a statement committed whose acknowledgement was lost is retried and
the rows land twice, and the source tables keep the duplicate rather than pretending
to be exactly-once.

Every statement is **parameterised**: every value is bound as a `?` parameter and
no value is interpolated into SQL text — only identifiers (table/column names) are.
Rows are batched into multi-row `INSERT ... VALUES (...), (...)` statements rather
than sent one statement per row: the connector's `executemany` costs a full round
trip each, measured at 1.7–2.0 s per row against this warehouse, so a 200-user load
spent five and a half minutes on round trips alone. With batching that same load
wrote 200 users in 30.5 s, an 11× reduction. Each statement carries at most
`INSERT_CHUNK_ROWS = 50` rows *and* at most `INSERT_CHUNK_CHARS` (768 KiB) of bound
UTF-8 bytes, because DBSQL rejects a statement whose parameters total more than 1 MiB
and one long body can exhaust that on its own. Bytes, not characters: a comment body
is user text, and an emoji is four. Logging is the stdlib
`logging` module (no `print`); per-post progress is `DEBUG` and each run emits
per-phase summaries at `INFO`, controlled by `LOG_LEVEL` (default `INFO`).

Two consequences worth knowing:

- A post that ages out of both listings simply stops being refreshed. Its row stays,
  so the raw tables accumulate history; nothing prunes them on a schedule.
- The DELETE and INSERT are separate autocommitted statements, so a failure between
  them leaves that chunk's ids absent rather than restoring the previous rows. The
  interleaving is what bounds the damage: a chunk of at most 5,000 ids, all of them
  fetched by the run that failed, so the next run re-fetches and rewrites them. A
  single delete pass followed by one insert pass would have stranded every id at once,
  including rows older than the 26-hour read window that no later run can reach.

The entrypoints `raise` on failure instead of calling `sys.exit(1)`. A raised
exception is what makes Dagster record the asset materialization as failed, which is
the state the retry policy acts on; `sys.exit` ends the process without the asset
being retried.

`orchestration/src/reddit_pipeline_orchestration/definitions.py` turns the three
extractors into Python assets — `reddit_raw/r_posts`, `reddit_raw/r_comments`,
`reddit_raw/r_users` — and adds one `@dbt_assets` definition, `dbt_reddit`. Each
extractor asset carries the key Dagster derives for its `reddit_raw` source, so the
source spec merges into it and the staging models depend on the loads. Comments and
users declare `deps=[RAW_POSTS_KEY]` because they derive from the same post listings, so
the graph runs posts → comments/users → dbt. The single `dbt_reddit` asset runs
`dbt build` through `DbtCliResource` against the repo-owned `profiles/` directory:
with no `--select` that builds and tests every model, so the marts are rebuilt against
the rows just loaded and the step works on a warehouse where dbt has never run. The
dbt step authenticates with a token minted from the same Databricks CLI profile the
extractors use (`databricks_token()` in `ingestion/reddit/_common.py`) at the start of
each run, immediately before `dbt` is invoked; passing a token is deliberate, because
dbt-databricks' own `auth_type: oauth` is a browser flow that cannot complete
unattended. The four assets carry a three-retry policy with exponential backoff and
jitter, ported from the Prefect tasks, so a transient failure is retried before the
run fails.

Run the whole pipeline once, headless, from `orchestration/`:

```bash
dagster asset materialize -m reddit_pipeline_orchestration.definitions --select '*'
```

### Scheduling

One schedule: `daily_edition_refresh`, cron `0 5 * * *` UTC, driving the
`reddit_refresh` job. It ships **stopped**, matching `daily_job_board_refresh` in the
sibling project, so a deployment does not write to the warehouse until someone flips
it in the UI. Until then a run is started from the UI or `dagster asset materialize`.

The cadence is daily because the `daily/` marts are incremental on `edition_date`: a
second build inside the same day replaces that day's page rather than appending to it,
so the app reads exactly one snapshot per day. 05:00 UTC sits after the American
evening and before the European morning, which is when the widest set of the previous
day's stories has been posted.

The extractors read the **whole seed**: `seeds/sections.csv` is both the section
map and the ingest list, so adding a subreddit is a one-line seed change that cannot
drift between ingest and marts. Each subreddit costs two listing requests — `new`
for what is being posted now, `top(day)` for what has already earned its place — and
both land in `workspace.topic_raw.r_posts`. Comments cover the 200 most-commented
in-window posts only, because comment fetching is one request per post; users are
capped at 200 profiles. The seed's `weight` column is still unused by any model.

## Macros

`macros/add_audit_fields.sql` defines `add_audit_fields()`, which adds the audit
column `rec_updated_at` to whichever model calls it. It is typed as a Databricks
`TIMESTAMP`:

```sql
select
    *,
    {{ add_audit_fields() }}
from {{ ref('some_model') }}
```

Under a `table` materialization the value is the time the dbt run wrote the row;
under a `view` the expression is evaluated per query, so it reports query time
rather than load time. Add further audit columns inside the macro and every model
that calls it picks them up.

All eight marts — the three original ones and the five in `models/marts/reddit/daily/`
— call `add_audit_fields()`. sqlfluff resolves the macro
because `.sqlfluff` sets `load_macros_from_path = macros`; without it, linting a
model that calls the macro fails on an undefined macro.

`macros/window_hours.sql` defines `window_hours()`, which returns `26` — the
dbt-side definition of the ingestion read window. `mart_comment_history` uses
it as `interval {{ window_hours() }} hours`, so the SQL window and
`INCREMENTAL_WINDOW_HOURS` in `ingestion/reddit/_common.py` stay in step;
`tests/test_window_contract.py` renders the macro and asserts the two agree.

It is a macro rather than a dbt `var` on purpose: sqlfluff's jinja templater stubs
`var()` with a placeholder token, which makes that `interval` expression
unparsable and fails `sqlfluff lint`. A macro renders normally through
`load_macros_from_path`. The value has no second definition in `dbt_project.yml`.

`macros/story_domain.sql` defines `story_domain(column)`, the publishing domain of a
story link reduced to its first label — `bbc`, `pravda`, `cnbc`. A self post's url is
its own reddit.com permalink, which is not an outlet, so it collapses to the empty
string rather than to `reddit`. Used by `mart_daily_edition`, which carries the result
through to the section summary's lead story.

## Linting, type checking, debugging

| Scope | Tool | Config |
| --- | --- | --- |
| `models/**/*.sql`, `macros/**/*.sql` | sqlfluff | `.sqlfluff` |
| `*.py` lint + format | ruff | `pyproject.toml` |
| `*.py` types | pyright | `pyproject.toml` |
| `*.py` debugger | debugpy | — |
| extractors, orchestration, window contract | pytest | `tests/` |

sqlfluff runs with the `databricks` dialect and the plain `jinja` templater
(`apply_dbt_builtins = True`), so `{{ ref() }}`/`{{ source() }}` resolve to stubs
and linting needs no dbt install, profile, or warehouse connection.
`load_macros_from_path = macros` makes `{{ add_audit_fields() }}` and
`{{ window_hours() }}` resolve to the project macros, so a model that calls them
lints without dbt. `target/`, `dbt_packages/` and `logs/` are excluded by
`ignore_paths` in `.sqlfluff`.

Install the pinned versions **into the active Python environment**:
```bash
pip install -r requirements-dev.txt
```
Do not also install them as `uv tool`s: `~/.local/bin` precedes the pyenv shims on
`PATH`, so the tool copies would shadow the environment ones.

Run:
```bash
sqlfluff lint                                    # report SQL violations (models/ and macros/)
sqlfluff fix                                     # apply SQL auto-fixes
ruff check .                                     # report Python violations
ruff check --fix .                               # apply Python auto-fixes
ruff format .                                    # format Python and the Python fences in *.md
pyright                                          # type check Python
pytest                                           # offline extractor/orchestration/window tests
DBT_PROFILES_DIR=profiles dbt parse              # parse the project (no warehouse needed)
```

`.github/workflows/ci.yml` triggers on push to `main` and on every pull request
and has two jobs:

- `lint`, on `ubuntu-latest` with Python 3.10, installs both requirements files
  plus `-e orchestration` and fails on any non-zero exit from
  `ruff format --check .`, `ruff check .`, `sqlfluff lint`, `pyright`,
  `dbt parse --profiles-dir profiles` (dummy `DATABRICKS_*` env — a parse needs no
  warehouse) and `pytest`.
- `workflow-lint` runs `rhysd/actionlint:1.7.7` to lint the workflow files
  themselves.

`pyright` is clean once the extraction runtimes from `requirements.txt` are
installed, which is what lets it resolve `praw`, `databricks-sql-connector`,
`databricks-sdk` and the `dagster` packages. On a checkout where they are missing it reports
`reportMissingImports` for each unresolved import; installing them clears that.
`python-dotenv` is pinned in `requirements.txt` because the extractors genuinely read
`.env`, so `import dotenv` resolves once the runtime requirements are installed.

Debug an extraction script (attach over DAP, e.g. from VS Code or an
`xd://debug`-capable client):
```bash
python -m debugpy --listen 5678 --wait-for-client -m ingestion.reddit.extract_reddit
```

## Agent skills

Model-facing capability packs live in `.agents/skills/<name>/SKILL.md` — one
directory per skill, discovered non-recursively by the `agents` provider:

| Skill | Covers |
| --- | --- |
| `lint-and-typecheck` | toolchain commands, config, intentional rule deviations, PATH pitfalls |
| `dbt-model-conventions` | layer responsibilities and materialisation, naming, sources, the project macros, test placement, remaining structural gaps |
| `reddit-extraction` | entrypoints and the Dagster job, what each extractor loads, `.env`, the shared `_common.py` scaffolding, the 26-hour read filter, the key-based upsert all three loads share, stubbing recipe for offline tests |

Check what is discovered:
```bash
omp skill list
```
