# Reddit Data Pipeline

This dbt project is designed to transform and model Reddit data on Databricks.

## Architecture & Flow
1. **Extraction**: Fetch raw Reddit data (hot posts, their comments, and user profiles) from r/worldnews with PRAW.
2. **Load**: Insert the rows into Databricks Delta tables incrementally (a rolling 5-hour window for posts/comments, a key-based upsert for users).
3. **Transformation (dbt)**: Model and clean raw data into staging, intermediate, and marts layers.
4. **Orchestration**: a Prefect flow runs the extractors in order, then the dbt mart build.

## Project Structure
- `models/staging/`: Raw source data cleaning, renaming, and type casting.
- `models/intermediate/`: Complex joins, aggregations, and business logic.
- `models/marts/`: Final analytics-ready tables for consumption (e.g. sentiment analysis, post metrics, user activity). Includes `mart_comment_history`, a comment-id-grain incremental mart that combines `r_comments` with its `r_worldnews` post context over a rolling 5-hour window.
- Each layer keeps its models in a source-named subfolder — currently `reddit/`, so a second source can sit beside it: `models/staging/reddit/`, `models/intermediate/reddit/`, `models/marts/reddit/`. The layer's materialisation in `dbt_project.yml` is inherited by the subfolder.
- `ingestion/reddit/`: the Python extractors that fetch Reddit data and load it into Databricks — one module per entity, plus `_common.py` holding the scaffolding they share (constants, env validation, clients, table setup, the two load strategies). Like the model layers, `ingestion/` groups extractors into a source-named subpackage — currently `reddit/`; `ingestion/__init__.py` and `ingestion/reddit/__init__.py` are package markers.
- `flows/`: Prefect orchestration. `reddit_extract_flow.py` runs the three extractors and then calls `dbt_build_flow.py`, whose `run-dbt-build` task runs the dbt build; the split keeps the two halves in separate flows, and `flows/` stays outside `ingestion/`.
- `tests/`: the offline pytest suite — `conftest.py` fixtures, `fakes.py` test doubles, and the extractor/flow/window-contract tests. No network and no warehouse.
- `profiles/profiles.yml`: the repo-owned dbt profile (see Quick Start).
- `macros/`: Reusable Jinja macros, e.g. `add_audit_fields()` and `window_hours()`.
- `.env.example`: the configuration template; copy it to `.env` (gitignored) and fill it in.
- `dbt_project.yml`: project config — `model-paths: ["models"]`, `macro-paths: ["macros"]`, the standard `analysis-paths`/`test-paths`/`seed-paths`/`snapshot-paths` entries and `clean-targets`. Materialisation is set per layer, and the rolling-window constant lives in the `window_hours()` macro rather than as a dbt variable.
- `tests/` is both dbt's default singular-test directory and the pytest suite's directory. That overlap is harmless: dbt only collects `.sql` files from `test-paths`, so the Python suite is invisible to it. `analyses/`, `seeds/` and `snapshots/` are the dbt defaults and hold only `.gitkeep`.

## Quick Start

### Prerequisites
- Python 3.10+
- The pipeline requirements, which include `dbt-databricks` and therefore the
  `dbt` CLI:
  ```bash
  pip install -r requirements.txt
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
`env_var(...)`, and the flow points dbt at it with `--profiles-dir profiles`.
The token is derived from the **same Databricks CLI profile the extractors use**,
so one `databricks auth login` covers both ingestion and the mart build:
```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
```
`DATABRICKS_CONFIG_PROFILE` selects a non-`DEFAULT` CLI profile. See
[Orchestration](#orchestration) for how the flow injects the token.

A hand-run `dbt` from the repo root still resolves `~/.dbt/profiles.yml`, whose
PAT may be invalid. Point manual runs at the repo profile and export the same
values the flow would:
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

The extractors and the flow read a single `.env` at the project root
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

Runs are orchestrated with Prefect. Install the pipeline runtime dependencies
**into the active Python environment**:
```bash
pip install -r requirements.txt
```

Prefect defaults to an *ephemeral* server: it starts a temporary local server and
SQLite backend for the duration of the process, so flows run with no persistent
service and no Prefect Cloud account. `prefect version` reports
`Server type: ephemeral`, and is the quickest check that the install works.

For the dashboard and retained run history, run the open-source server instead —
still local, still no account or license:
```bash
prefect server start --background   # foreground: `prefect server start`
prefect profile create local
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api --profile local
prefect profile use local           # revert with `prefect profile use ephemeral`
```
`prefect version` then reports `Server type: server` and the dashboard is at
<http://127.0.0.1:4200>. State lives in `~/.prefect/prefect.db` (the same SQLite
file the ephemeral server uses); `prefect server stop` shuts the server down.

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

### Extraction and mart-build flow

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
exception is what makes Prefect record the task as `Failed`, which is the state the
retry policy acts on; `sys.exit` would be recorded as `Crashed` and never retried.

`flows/reddit_extract_flow.py` orchestrates the three extractors as Prefect tasks
— `fetch-posts`, `fetch-comments`, `fetch-users`, in that order (comments and
users derive from the same hot posts, so the order matters) — then triggers
`flows/dbt_build_flow.py`. Its `run-dbt-build` task runs `dbt build --profiles-dir
profiles` from the repo root: with no `--select` that builds and tests every model
in the project, so the marts are rebuilt against the rows just loaded — the step
works even on a warehouse where dbt has never run. If `dbt` is not on PATH the
task raises a `RuntimeError` naming the fix rather than failing obscurely. The
dbt step authenticates with a token the task derives from the same Databricks CLI
profile the extractors use (`databricks.sdk.core.Config(profile=…).authenticate()`)
and passes into the subprocess environment together with the host/http_path from
`.env`; passing a token is deliberate, because dbt-databricks' own
`auth_type: oauth` is a browser flow that cannot complete unattended. The extract
flow calls the dbt flow as a nested flow run — `dbt-build` has no deployment of
its own — so a failed build fails the extract run. Both flows import their code as
a package, so run them from the repo root. Each task retries 3 times with delays
of 10s, 60s and 300s, plus jitter. Run it with:

```bash
python -m flows.reddit_extract_flow
```

### Deploying to the local server

`prefect.yaml` defines one deployment, `reddit-extract`, on the `reddit-pool`
process work pool. With the local server running (see [Orchestration](#orchestration)),
register it and start a worker — both from the repo root:
```bash
prefect deploy -n reddit-extract
prefect worker start --pool reddit-pool
```
The deployment carries no schedule. Trigger a run by hand or from the dashboard:
```bash
prefect deployment run reddit-extract/reddit-extract
```
`reddit-extract` still calls `dbt-build` as a subflow, so this one deployment runs
the whole pipeline. Its pull step points the worker at this checkout — the path
`prefect deploy init` generates — so the worker must run on the machine that holds
the repo, its `.env` and the Databricks CLI login.

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
| extractors, flow, window contract | pytest | `tests/` |

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
pytest                                           # offline extractor/flow/window tests
DBT_PROFILES_DIR=profiles dbt parse              # parse the project (no warehouse needed)
```

`.github/workflows/ci.yml` triggers on push to `main` and on every pull request
and has two jobs:

- `lint`, on `ubuntu-latest` with Python 3.10, installs both requirements files
  and fails on any non-zero exit from `ruff format --check .`, `ruff check .`,
  `sqlfluff lint`, `pyright`, `dbt parse --profiles-dir profiles` (dummy
  `DATABRICKS_*` env — a parse needs no warehouse) and `pytest`.
- `workflow-lint` runs `rhysd/actionlint:1.7.7` to lint the workflow files
  themselves.

`pyright` is clean once the extraction runtimes from `requirements.txt` are
installed, which is what lets it resolve `praw`, `databricks-sql-connector` and
`databricks-sdk`. On a checkout where they are missing it reports
`reportMissingImports` for each unresolved import; installing them clears that.
`python-dotenv` is already present because Prefect depends on it.

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
| `reddit-extraction` | entrypoints and the Prefect flow, what each extractor loads, `.env`, the shared `_common.py` scaffolding, the rolling-window loads for posts/comments and the key-based upsert for users, stubbing recipe for offline tests |

Check what is discovered:
```bash
omp skill list
```
