# Reddit Data Pipeline

This dbt project is designed to transform and model Reddit data on Databricks.

## Architecture & Flow
1. **Extraction**: Fetch raw Reddit data (hot posts, their comments, and user profiles) from r/worldnews with PRAW.
2. **Load**: Insert the rows into Databricks Delta tables incrementally (a rolling 5-hour window for posts/comments, a key-based upsert for users).
3. **Transformation (dbt)**: Model and clean raw data into staging, intermediate, and marts layers.
4. **Orchestration**: a Dagster job runs the extractors in order, then one `dbt build`.

## Project Structure
- `models/staging/`: Raw source data cleaning, renaming, and type casting.
- `models/intermediate/`: Complex joins, aggregations, and business logic.
- `models/marts/`: Final analytics-ready tables for consumption (e.g. sentiment analysis, post metrics, user activity). Includes `mart_comment_history`, a comment-id-grain incremental mart that combines `r_comments` with its `r_worldnews` post context over a rolling 5-hour window.
- Each layer keeps its models in a source-named subfolder — currently `reddit/`, so a second source can sit beside it: `models/staging/reddit/`, `models/intermediate/reddit/`, `models/marts/reddit/`. The layer's materialisation in `dbt_project.yml` is inherited by the subfolder.
- `ingestion/reddit/`: the Python extractors that fetch Reddit data and load it into Databricks — one module per entity, plus `_common.py` holding the scaffolding they share (constants, env validation, clients, table setup, the two load strategies). Like the model layers, `ingestion/` groups extractors into a source-named subpackage — currently `reddit/`; `ingestion/__init__.py` and `ingestion/reddit/__init__.py` are package markers.
- `orchestration/`: the Dagster code location (`reddit_pipeline_orchestration.definitions`). Three extractor assets and one dbt-assets definition, merged into a single graph by asset key; see [Orchestration](#orchestration).
- `tests/`: the offline pytest suite — `conftest.py` fixtures, `fakes.py` test doubles, and the extractor/definitions/asset/window-contract tests. No network and no warehouse.
- `profiles/profiles.yml`: the repo-owned dbt profile (see Quick Start).
- `macros/`: Reusable Jinja macros, e.g. `add_audit_fields()` and `window_hours()`.
- `.env.example`: the configuration template; copy it to `.env` (gitignored) and fill it in.
- `dbt_project.yml`: project config — `model-paths: ["models"]`, `macro-paths: ["macros"]`, the standard `analysis-paths`/`test-paths`/`seed-paths`/`snapshot-paths` entries and `clean-targets`. Materialisation is set per layer, and the rolling-window constant lives in the `window_hours()` macro rather than as a dbt variable.
- `tests/` is both dbt's default singular-test directory and the pytest suite's directory. That overlap is harmless: dbt only collects `.sql` files from `test-paths`, so the Python suite is invisible to it. `analyses/`, `seeds/` and `snapshots/` are the dbt defaults and hold only `.gitkeep`.

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
validation, the Reddit and Databricks clients, table setup, and the two load
strategies described below.

Loads are incremental, not full refreshes, and each writes a trailing
`ingested_at TIMESTAMP` column stamped at load time (the UTC time the row was
written, not a value Reddit supplied):

- `ingestion/reddit/extract_reddit.py` and
  `ingestion/reddit/extract_reddit_comments.py` load
  a **rolling 5-hour window** on the entity's own `created_utc`
  (`INCREMENTAL_WINDOW_HOURS = 5`). Each run deletes only the rows inside that
  window (`DELETE FROM <table> WHERE created_utc >= ?`) and inserts only
  the fetched rows that fall inside it. Older rows are retained and never
  rewritten, so both tables accumulate history instead of being replaced. If
  the fetch contains nothing inside the window, the run skips both the delete
  and the insert. Their entrypoints return the in-window row count.
- `ingestion/reddit/extract_reddit_users.py` has no window — an account's `created_utc`
  is its account age, not recency. It is a key-based upsert: `DELETE WHERE id IN
  (<fetched ids>)` followed by an insert of the fetched rows, so returning users
  get refreshed karma/gold and new users are added, with no duplicates. Per-author
  profile failures (suspended, deleted, private) are counted, logged at `WARNING`,
  and reported in the run summary rather than swallowed.

Both strategies are **parameterised**: every value is bound as a `?` parameter via
`executemany` and no value is interpolated into SQL text — only identifiers
(table/column names) are. Logging is the stdlib `logging` module (no `print`);
per-post progress is `DEBUG` and each run emits per-phase summaries at `INFO`,
controlled by `LOG_LEVEL` (default `INFO`).

Two consequences worth knowing:

- Because the delete is scoped to the window and the source is a bounded fetch, a
  post or comment that ages out of the source while still inside the window is
  deleted and not re-inserted unless the fetch still returns it.
- The DELETE and INSERT are separate autocommitted statements, so a failure
  between them leaves the window rows absent rather than restoring the previous
  snapshot. A retry re-fetches and rewrites the window.

The entrypoints `raise` on failure instead of calling `sys.exit(1)`. A raised
exception is what makes Dagster record the asset materialization as failed, which is
the state the retry policy acts on; `sys.exit` ends the process without the asset
being retried.

`orchestration/src/reddit_pipeline_orchestration/definitions.py` turns the three
extractors into Python assets — `reddit_raw/r_worldnews`, `reddit_raw/r_comments`,
`reddit_raw/r_users` — and adds one `@dbt_assets` definition, `dbt_reddit`. Each
extractor asset carries the key Dagster derives for its `reddit_raw` source, so the
source spec merges into it and the staging models depend on the loads. Comments and
users declare `deps=[RAW_POSTS_KEY]` because they derive from the same hot posts, so
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

There is no schedule, matching the Prefect deployment, which was triggered on demand.
To add one, define a stopped `dg.ScheduleDefinition` for the `reddit_refresh` job in
`definitions.py` and add it to `dg.Definitions(schedules=[...])`; it stays inert until
switched on in the UI. Until then a run is started from the UI or `dagster asset
materialize`, so nothing writes to the warehouse on its own.

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

All three marts — `mart_comment_history`, `mart_post_summary` and
`mart_user_metrics` — call `add_audit_fields()`. sqlfluff resolves the macro
because `.sqlfluff` sets `load_macros_from_path = macros`; without it, linting a
model that calls the macro fails on an undefined macro.

`macros/window_hours.sql` defines `window_hours()`, which returns `5` — the
dbt-side definition of the ingestion rolling window. `mart_comment_history` uses
it as `interval {{ window_hours() }} hours`, so the SQL window and
`INCREMENTAL_WINDOW_HOURS` in `ingestion/reddit/_common.py` stay in step;
`tests/test_window_contract.py` renders the macro and asserts the two agree.

It is a macro rather than a dbt `var` on purpose: sqlfluff's jinja templater stubs
`var()` with a placeholder token, which makes that `interval` expression
unparsable and fails `sqlfluff lint`. A macro renders normally through
`load_macros_from_path`. The value has no second definition in `dbt_project.yml`.

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
| `reddit-extraction` | entrypoints and the Dagster job, what each extractor loads, `.env`, the shared `_common.py` scaffolding, the rolling-window loads for posts/comments and the key-based upsert for users, stubbing recipe for offline tests |

Check what is discovered:
```bash
omp skill list
```
