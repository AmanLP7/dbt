# Job Postings Pipeline

An ELT pipeline that fetches open job listings from **keyless** job-board APIs,
lands them verbatim in a Databricks Delta table, and does the role filtering
downstream in dbt.

## Why the filtering lives in dbt

The three sources are firehoses. None of them honours a role or keyword query —
they accept a `?search=`-style parameter and ignore it (a query for `kubernetes`
returns "Quality Control Manager" and "Legal Assistant") — and none of them is
role-scoped to begin with. So the extractor pages through each board and writes
every listing to `raw_job_postings` unchanged; choosing which listings count as a
role is a SQL concern, applied in `models/marts/jobs/` against the `role_keywords`
seed. The raw table therefore stays a faithful record of what each source said,
and the definition of "a data engineer job" is a three-line CSV edit rather than
an extractor release.

## Architecture

1. **Extraction** — `ingestion/jobs/fetch_job_postings.py` fetches remotive,
   arbeitnow and jobicy (plain HTTP JSON, no API key, no account) and normalises
   each payload into one shared schema. arbeitnow and jobicy paginate and cap
   their page size server-side, so each is walked `MAX_PAGES_PER_SOURCE` pages
   deep — newest first, paced between requests — and remotive, which does not
   paginate, is fetched in a single request.
2. **Raw** — rows are appended to `workspace.job_postings.raw_job_postings` with an
   `ingested_at` stamp. Append-only: a posting that scrolls off a source's rolling
   window simply stops appearing, so history accumulates instead of being
   overwritten. A posting a source published without an id is dropped at this step
   (the raw table's key is `source_job_id`, and staging dedupes on it). A source
   that fails — network error, shape change, unparsable payload — is logged and
   skipped; the run aborts only if all three fail.
3. **Staging** — `stg_job_postings` (view) keeps the newest fetch of each
   `(source, source_job_id)`, builds `job_key`, normalises `title`, unpacks the
   tags JSON array, strips HTML from the description, and nulls out blank strings.
4. **Marts** — `mart_role_listings` (table) joins listings to `role_keywords` on
   `title_normalized like '%' || keyword || '%'`; one posting can match several role
   families, so the model emits one row per (role family, posting).
   `mart_role_summary` (table) aggregates that to counts per role family.

## Project structure

- `models/staging/`: source-faithful cleaning — dedupe, trim, unnest, rename.
- `models/marts/`: the role filter and its summary, materialised as tables.
- Each layer keeps its models in a source-named subfolder — currently `jobs/`, so a
  second board source can sit beside it: `models/staging/jobs/`,
  `models/marts/jobs/`. The layer's materialisation in `dbt_project.yml` is
  inherited by the subfolder.
- `ingestion/jobs/`: the extractor package — `fetch_job_postings.py` (the three API
  clients and their normalisers) and `_common.py` (connection, table setup, the
  chunked insert). Like the model layers, `ingestion/` groups extractors into a
  source-named subpackage — currently `jobs/`.
- `seeds/role_keywords.csv`: the role taxonomy the marts join against.
- `macros/add_audit_fields.sql`: `add_audit_fields()`, adding `rec_updated_at`.
- `tests/`: the offline pytest suite (`conftest.py`, `fakes.py`, and the extractor /
  raw-table / seed-contract tests). No network and no warehouse.
- `profiles/profiles.yml`: the repo-owned dbt profile (see Quick Start).
- `.env.example`: the configuration template; copy it to `.env` (gitignored) and
  fill it in.
- `dbt_project.yml`: project config. Materialisation is set per layer.
- `tests/` is both dbt's default singular-test directory and the pytest suite's
  directory. That overlap is harmless: dbt only collects `.sql` files from
  `test-paths`, so the Python suite is invisible to it.

## Quick Start

### Prerequisites

Python 3.10+, and the pipeline requirements (which include `dbt-databricks` and
therefore the `dbt` CLI):

```bash
pip install -r requirements.txt
```

Install into the active Python environment rather than as a `uv tool`: `~/.local/bin`
precedes the pyenv shims on `PATH`, so tool copies would shadow the environment's
`dbt` and `python`.

Configuration comes from the template:

```bash
cp .env.example .env
```

`.env` is gitignored, and any required value left at a `your_...` placeholder is
rejected at startup, so a half-configured copy fails loudly. See
[Credentials](#credentials).

### Profile setup

The repo carries its own dbt profile at `profiles/profiles.yml`, and a `profiles.yml`
symlink beside `dbt_project.yml` makes a bare `dbt` inside this directory resolve it
instead of `~/.dbt/profiles.yml` (which still describes the previous FREE workspace).
Either way the warehouse (catalog `workspace`, schema `job_postings`) comes from the
profile, while `host`, `http_path` and `token` come from `env_var(...)` — the same host
and path the extractor connects with, so ingestion and the models cannot disagree about
which workspace they write to. The token is the one value the two halves source
differently: `dbt` has no way to accept a credentials provider, so the orchestrated
run mints a token from the CLI profile at the start of each `dbt build`, while a
hand-run needs `DATABRICKS_TOKEN` exported first.

`dbt` does not read `.env`; only the extractor loads it. To run dbt against the repo
profile with the values from `.env`:

```bash
set -a; source .env; set +a
export DATABRICKS_TOKEN=$(databricks auth token --output json | jq -r .access_token)
DBT_PROFILES_DIR=profiles dbt build
```

### Running the pipeline

```bash
# 1. Land the raw rows (fetches the three boards, appends to raw_job_postings).
python -m ingestion.jobs.fetch_job_postings

# 2. Seed the role taxonomy, build and test the models.
dbt build
```

Both steps also run as one ordered job under Dagster — extraction, then the
`dbt build` — which is the reason the extractor can be the dbt source; see
[Orchestration (Dagster)](#orchestration-dagster).

`dbt build` runs the `role_keywords` seed, then the models, then their tests, in
dependency order. To see the result:

```bash
dbt show --select mart_role_summary --limit 20
```

One observed run — three ingest batches, 1320 raw rows, 449 deduped listings in
staging:

| role_family | open_listings | hiring_companies | sources |
| --- | --- | --- | --- |
| machine learning engineer | 10 | 10 | 2 |
| data engineer | 9 | 9 | 2 |
| data scientist | 5 | 4 | 2 |
| data analyst | 1 | 1 | 1 |

Counts move with whatever the boards are advertising that day; the numbers above
exist to show the shape, not as an expectation. Note what `open_listings` counts:
staging keeps the newest fetch of each `(source, source_job_id)` and nothing
expires, so a listing that scrolls off a board's rolling window stays in the table
for good. The number is "postings seen and not re-listed differently", not "jobs
open today" — a recency filter would be a change to `stg_job_postings`, not a
presentation detail.

## Orchestration (Dagster)

The same two steps also run as one ordered job under Dagster, which is what lets
the extractor be the upstream of `stg_job_postings`: a single run lands the raw
rows and then runs `dbt build`. The layer lives in `orchestration/` — a package
(`job-postings-orchestration`) whose code location is
`orchestration/src/job_postings_orchestration/definitions.py`, exporting `defs`. The
extractor and the dbt project stay where they are; nothing moved.

- Two tasks in one job, in this order: `jobs/raw_job_postings` (the extractor, group
  `ingestion`) then `dbt_job_postings` (one `dbt build`: seed → staging → marts →
  tests, yielding the `role_keywords` seed, `stg_job_postings`, `mart_role_listings`
  and `mart_role_summary`, all four grouped as `transformation`).
- 5 assets — `jobs/raw_job_postings` plus those four dbt assets — 14 asset checks
  (the 10 column tests declared on the models, plus the 4 declared on the
  `jobs.raw_job_postings` source), one job `job_board_refresh`, and one schedule
  `daily_job_board_refresh` (cron `0 6 * * *`), created stopped.
- Install all of it into the active Python environment:
  `pip install -r requirements.txt -r requirements-dev.txt -e orchestration`. The
  `dagster` CLI, `job_postings_orchestration`, the extractor and `dbt` resolve from
  that one interpreter, with the package installed editable. `dagster==1.13.26`,
  `dagster-dbt==0.29.26` and `dagster-webserver==1.13.26` (the last is what
  `dagster dev` needs to start its UI; `dagster` does not pull it in) are runtime
  pins in the root `requirements.txt`;
  `orchestration/pyproject.toml` defines the package (its name and its
  `[tool.dagster]` module name) and pins nothing.
- It uses the committed `profiles/profiles.yml` (`profile="job_postings"`,
  `target="dev"`) and the same `.env` as the extractor. Those variables must be
  present when the code location loads, because the profile resolves `env_var(...)`
  while the manifest is parsed.

```bash
# UI plus scheduler/daemon; code location auto-detected from [tool.dagster].
cd orchestration && dagster dev

# Headless: extract, then dbt build, once. Only `dagster dev` auto-detects the
# code location from [tool.dagster], so here it is named with -m.
cd orchestration && dagster asset materialize -m job_postings_orchestration.definitions --select '*'
```

Full detail — install, the source-test-to-check wiring, and the stopped schedule —
is in [orchestration/README.md](orchestration/README.md). `pytest` stays the offline
pre-merge gate; dbt's tests need a warehouse, so they run inside the orchestrated
`dbt build` (or `dbt build` by hand).

## Credentials

The extractor reads a single `.env` at the project root (`load_dotenv()`), never a
committed file. `.env.example` is the template:

| Key | Required | Purpose |
| --- | --- | --- |
| `DATABRICKS_HOSTNAME` | yes | Workspace host, e.g. `https://<workspace>.cloud.databricks.com`. Both halves of the pipeline accept it with or without the `https://` scheme and with or without a trailing slash: the extractor strips them in `connect()`, and `profiles/profiles.yml` strips them before handing the host to dbt |
| `DATABRICKS_HTTP_PATH` | yes | HTTP path of the SQL warehouse |
| `DATABRICKS_CONFIG_PROFILE` | no | CLI profile in `~/.databrickscfg` the pipeline authenticates through; default `DEFAULT` |
| `LOG_LEVEL` | no | Extractor log level; default `INFO` |
| `JOB_POSTINGS_CATALOG` | no | Overrides the catalog of the raw table *and* of the `jobs` dbt source; default `workspace` (the profile's catalog) |
| `JOB_POSTINGS_SCHEMA` | no | Overrides the schema of the raw table *and* of the `jobs` dbt source; default `job_postings` (the profile's schema) |

The job sources need **no** credentials: remotive, arbeitnow and jobicy are public
JSON endpoints. Everything Databricks needs comes from one CLI login:

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
```

`DATABRICKS_TOKEN` is deliberately **not** read from the environment. The extractor
hands the SQL connector a credentials provider backed by that profile, and the
orchestrated dbt step mints a token from it at the start of each run, so nothing
holds a credential that has to be refreshed by hand. A token exported into a shell
lasts one hour; a long-lived process that captured one at startup — a Dagster code
server, for instance — would open every session after the first hour with a dead
credential, which is what `RequestError: refresh the databricks token` was. Setting
`DATABRICKS_TOKEN` by hand is only needed to run `dbt` directly, because
`profiles/profiles.yml` requires it:

```bash
export DATABRICKS_TOKEN=$(databricks auth token --output json | jq -r .access_token)
```

`databricks auth token` prints a `Databricks skills are not installed...` notice on
stderr; it is harmless.

## The raw table

`ingestion/jobs/_common.py` creates the schema and the table if they are missing, so
the first run works on an empty warehouse. Column order is fixed by `RAW_COLUMNS`,
which the DDL, the insert prefix and the staging model all agree on.

Loads are chunked: the connector issues one statement per parameter set for
`INSERT ... VALUES`, so a row-per-statement `executemany()` costs a round trip per
row (measured at ~2s each). Rows are instead packed `INSERT_CHUNK_ROWS = 50` per
statement — a single-page 440-row run landed in about 24 seconds across 9
statements; a run at the current page cap batches roughly 1950 rows into ~39.
Every value is bound as a `?` parameter; only the table name is interpolated.

The row count alone is not a safe bound. DBSQL rejects a statement whose bound
parameters total more than 1048576 characters, and a posting's `description` is
often tens of KB of source HTML, so 50 of them overflow it: the first run on the
2026-10-24 workspace failed with `Parameterized query's parameters are too large:
the combined size of parameters is 2059161, exceeding the limit of 1048576
characters`. `chunk_rows()` therefore splits on whichever limit is reached first —
`INSERT_CHUNK_ROWS` rows or `INSERT_CHUNK_CHARS` (three quarters of the server
limit) of parameter text. A row larger than the whole budget still goes alone and
will still be rejected; nothing can shorten it without corrupting the raw record.

## Role keywords

`seeds/role_keywords.csv` is the definition of the taxonomy:

```csv
role_family,keyword
data engineer,data engineer
data engineer,analytics engineer
machine learning engineer,ml engineer
...
```

Adding a row adds a role family (or widens an existing one) on the next `dbt build`.
A keyword is matched as a case-insensitive substring of the listing title, so
`etl` catches "ETL Developer" and `analytics engineer` catches "Senior Analytics
Engineer (Remote)"; a keyword containing SQL `LIKE` wildcards (`%`, `_`) would match
far more than intended and is rejected by `tests/test_seed_contract.py`. The match
has no word boundary, so it also fires inside a longer word: `ai engineer` matches
"Dubai engineer" as well. Multi-word keywords that would read as a different role
are worth a look in the `mart_role_listings` output after seeding a new one.

## Tests

`pytest` runs an offline suite — the HTTP layer and the warehouse are stubbed, so it
needs neither network access nor credentials. It does need the dbt manifest, which
`tests/test_definitions.py` reads through the Dagster code location: on a fresh
checkout run `DBT_PROFILES_DIR=profiles dbt parse` first (dummy `DATABRICKS_*`
values are enough). CI does the two steps in that order.

- `tests/test_extractors.py`: each source's payload → raw row mapping, timestamp
  normalisation (ISO, `Z`, offsets, epochs, garbage), the fail-soft loop
  (one broken source is skipped, all three failing aborts before connecting), and
  the drop of postings that arrive without a `source_job_id`.
- `tests/test_raw_table.py`: statement chunking (row and parameter-size bounds),
  bound parameters, one ingest stamp per run, and the DDL/column-list contract.
- `tests/test_seed_contract.py`: seed shape and the `LIKE`-wildcard guard.
- `tests/test_definitions.py`: the Dagster code location — the asset set, the
  extractor→staging merge, all 14 asset checks (including the 4 on the
  `jobs.raw_job_postings` source), the job's selection and the stopped schedule.
- `tests/test_extract_asset.py`: the extractor asset — the `rows_written`
  materialization metadata and failure propagation.

dbt's own tests (in `models/**/*.yml`) run against the warehouse as part of
`dbt build` and cover the things only real data can: null keys, source values the
extractor is allowed to emit, and the referential integrity between
`mart_role_listings.role_family` and the seed.

## Linting, type checking

| Scope | Tool | Config |
| --- | --- | --- |
| `models/**/*.sql`, `macros/**/*.sql` | sqlfluff | `.sqlfluff` |
| `*.py` lint + format | ruff | `pyproject.toml` |
| `*.py` types | pyright | `pyproject.toml` |
| extractor, raw table, seed contract, orchestration definitions | pytest | `tests/` |

sqlfluff runs with the `databricks` dialect and the plain `jinja` templater
(`apply_dbt_builtins = True`), so `{{ ref() }}`/`{{ source() }}` resolve to stubs and
linting needs no dbt install, profile, or warehouse connection.
`load_macros_from_path = macros` makes `{{ add_audit_fields() }}` resolve to the
project macro. `references.keywords` ignores `source`, `title`, `location` and
`tags` — raw column names the Databricks dialect also spells as DDL clause keywords.

```bash
pip install -r requirements.txt -r requirements-dev.txt -e orchestration
sqlfluff lint
ruff check .
ruff format --check .
pyright
pytest
DBT_PROFILES_DIR=profiles dbt parse   # parses the project; no warehouse needed
# Dummy DATABRICKS_* values suffice: the profile resolves env_var(...) at parse time.
dagster definitions validate -m job_postings_orchestration.definitions
```
