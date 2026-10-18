# Orchestration (Dagster)

Dagster owns the ordering between the extractor and dbt. Both halves already run
standalone; the orchestration layer adds the one thing they cannot do apart — make
the extractor's output the dbt source, so a single run lands the raw rows and then
builds the models that read them.

## Layout

- `orchestration/pyproject.toml` — defines the package `job-postings-orchestration`:
  its name, build backend, wheel packages and `[tool.dagster] module_name`, with no
  dependency list of its own. The pins live in the root `requirements.txt` — the
  framework pair (`dagster==1.13.26`, `dagster-dbt==0.29.26`, a version-matched
  release train) plus `dagster-webserver==1.13.26`, which is what `dagster dev`
  needs to serve the UI (`dagster` alone refuses to start it), beside the
  dbt/Databricks stack (`dbt-databricks==1.12.6`,
  `databricks-sql-connector==4.4.0`, `python-dotenv==1.2.4`).
- `orchestration/src/job_postings_orchestration/definitions.py` — the code location,
  exporting `defs`.
- The extractor (`ingestion/`) and the dbt project stay where they are; nothing moved.

The dbt project *is* the repo root, so the definitions point it there:

```python
DbtProject(project_dir=<repo root>, profiles_dir=<repo root>/profiles,
           profile="job_postings", target="dev")
```

That is the committed profile, never `~/.dbt/profiles.yml`.

## Install

One install command covers the whole pipeline, into the active Python environment:

```bash
# Runtime pins, dev tooling and the orchestration package, in the active interpreter.
pip install -r requirements.txt -r requirements-dev.txt -e orchestration
```

The package installs editable into that same environment on purpose: the `dagster`
CLI, `job_postings_orchestration`, the extractor and the `dbt` CLI all have to
resolve from one interpreter, because the code location imports the extractor and
shells out to `dbt`. A separate per-project environment would hide one half from the
other. Editable (`-e`) keeps `definitions.py` the live file rather than a copy.

## The two tasks

One job, two tasks, in this order because the second reads what the first writes:

1. **`jobs/raw_job_postings`** (Python, group `ingestion`, kinds
   `python`/`databricks`) — calls `extract_job_postings()` from
   `ingestion/jobs/fetch_job_postings.py` and returns `rows_written` as
   materialization metadata.
2. **`dbt_job_postings`** (dbt) — one `dbt build` (seed → staging → marts → tests)
   through `DbtCliResource`, yielding the dbt assets `role_keywords` (seed),
   `stg_job_postings`, `mart_role_listings`, `mart_role_summary`.

The extractor asset is declared under the same asset key Dagster derives for the
dbt source `jobs.raw_job_postings` (`<source_name>/<table_name>`), so the two
definitions merge and the extractor becomes the upstream of `stg_job_postings`.
That merge is what makes one run extract and then build instead of doing either
half alone.

## Assets, checks, job, schedule

| Kind | Count | Names |
| --- | --- | --- |
| Assets | 5 | `jobs/raw_job_postings` (extractor), `role_keywords` (seed), `stg_job_postings`, `mart_role_listings`, `mart_role_summary` |
| Asset checks | 14 | the 10 column tests declared on the models, plus the 4 declared on the `jobs.raw_job_postings` source |
| Asset groups | 2 | `ingestion` (the extractor), `transformation` (the four dbt assets) |
| Job | 1 | `job_board_refresh` = `AssetSelection.all()` |
| Schedule | 1 | `daily_job_board_refresh`, cron `0 6 * * *` |

Groups are assigned in `definitions.py`: `group_name="ingestion"` on the extractor
asset, and `JobPostingsDbtTranslator.get_group_name()` returning `transformation` for
every dbt node. The translator deliberately returns `None` for dbt *sources*, because
the `jobs.raw_job_postings` source shares its asset key with the extractor and a
second group on that key would conflict with `ingestion`. Setting the group in
`dbt_project.yml` instead would need a matching dbt-side `groups:` definition for no
gain the UI can show. Both groups are selectable:
`dg.AssetSelection.groups("ingestion")` / `.groups("transformation")`.

The 10 model tests attach to their models on their own. The 4 source tests become
checks on the extractor only because the definitions set
`enable_source_tests_as_checks=True` in `DagsterDbtTranslatorSettings` — without it
the four tests on `jobs.raw_job_postings` would have nothing to attach to. With it,
the extractor's `source` values are a checked contract rather than a guess.

The schedule ships **stopped** (Dagster's `default_status` default), so nothing
writes to the warehouse until someone turns it on. Enable it in the Dagster UI
(Schedules → `daily_job_board_refresh` → on), or set `default_status=RUNNING` on
the `ScheduleDefinition` in the definitions.

## Commands

Run from `orchestration/`:

```bash
# UI plus scheduler/daemon; code location auto-detected from [tool.dagster] module_name.
dagster dev

# Headless: extract, then dbt build, once. No UI. `dagster dev` is the command that
# reads [tool.dagster]; `asset materialize` takes a workspace target, so name the
# module with -m (it works from any directory once the package is installed).
dagster asset materialize -m job_postings_orchestration.definitions --select '*'
```

`asset materialize` prints Dagster's first-run telemetry banner to stdout. Silence it
by creating a `$DAGSTER_HOME/dagster.yaml` containing `telemetry: {enabled: false}`.

## Environment

The code location reads the same `.env` as the extractor: `DATABRICKS_HOSTNAME`,
`DATABRICKS_HTTP_PATH`, the optional `DATABRICKS_CONFIG_PROFILE` and the optional
`LOG_LEVEL`. Those values must be present **when the code location loads**, not only
when dbt runs: `profiles/profiles.yml` resolves them through `env_var(...)` while the
dbt manifest is parsed, so a missing host or path fails at load time.

No token belongs in the environment. Both tasks authenticate from the CLI profile in
`~/.databrickscfg` (`databricks auth login`): the extractor hands the SQL connector a
credentials provider, and the dbt asset mints a token from the same profile at the
start of each run, immediately before invoking `dbt`. A token exported into a shell
dies after an hour, which would leave every run on a long-lived code server failing at
session open with `RequestError: refresh the databricks token`.

dbt does not read `.env` — only the extractor's `load_dotenv()` does — so to run
without relying on that loader, put the values in the environment first:

```bash
# Export .env into the shell so both dagster and dbt see the values.
set -a; source .env; set +a
cd orchestration && dagster dev
```

## Manual path

The orchestration layer is an alternative, not a replacement:

```bash
# 1. Land the raw rows.
python -m ingestion.jobs.fetch_job_postings

# 2. Seed, build and test the models against the committed profile.
DBT_PROFILES_DIR=profiles dbt build
```

## Tests

`pytest` stays the offline pre-merge gate — no network, no warehouse. It reads
`target/manifest.json` (through the code location), so on a fresh checkout run
`DBT_PROFILES_DIR=profiles dbt parse` first; dummy `DATABRICKS_*` values are
enough. dbt's tests need a warehouse, so they run as part of the orchestrated
`dbt build` (or by hand with `dbt build`).

Two modules in the offline suite cover this layer:

- `tests/test_definitions.py`: the code location's wiring — the asset set, the
  extractor→staging merge, all 14 asset checks (including the 4 on the
  `jobs.raw_job_postings` source), the job's selection and the schedule shipping
  stopped.
- `tests/test_extract_asset.py`: the extractor asset — the `rows_written`
  materialization metadata and failure propagation.
