# Orchestration (Dagster)

Dagster owns the ordering between the extractors and dbt. Both halves already run
standalone; the orchestration layer adds the one thing they cannot do apart — make
each extractor's output a dbt source, so a single run lands the raw rows and then
builds the models that read them.

## Layout

- `orchestration/pyproject.toml` — defines the package
  `reddit-pipeline-orchestration`: its name, build backend, wheel packages and
  `[tool.dagster] module_name`, with no dependency list of its own. The pins live in
  the root `requirements.txt` — the framework pair (`dagster==1.13.26`,
  `dagster-dbt==0.29.26`, a version-matched release train) plus
  `dagster-webserver==1.13.26`, which is what `dagster dev` needs to serve the UI
  (`dagster` alone refuses to start it), beside the dbt/Databricks stack
  (`dbt-databricks==1.12.6`, `databricks-sql-connector==4.4.0`,
  `python-dotenv==1.2.4`).
- `orchestration/src/reddit_pipeline_orchestration/definitions.py` — the code
  location, exporting `defs`.
- The extractors (`ingestion/`) and the dbt project stay where they are; nothing
  moved.

The dbt project *is* the repo root, so the definitions point it there:

```python
DbtProject(project_dir=<repo root>, profiles_dir=<repo root>/profiles,
           profile="reddit_pipeline", target="dev")
```

That is the committed profile, never `~/.dbt/profiles.yml`.

## Install

One install command covers the whole pipeline, into the active Python environment:

```bash
# Runtime pins, dev tooling and the orchestration package, in the active interpreter.
pip install -r requirements.txt -r requirements-dev.txt -e orchestration
```

The package installs editable into that same environment on purpose: the `dagster`
CLI, `reddit_pipeline_orchestration`, the extractors and the `dbt` CLI all have to
resolve from one interpreter, because the code location imports the extractors and
invokes dbt through `DbtCliResource`. A separate per-project environment would hide
one half from the other. Editable (`-e`) keeps `definitions.py` the live file rather
than a copy.

## The four tasks

One job, four assets, in this order because the later ones read what the earlier ones
write:

1. **`reddit_raw/r_worldnews`** (Python, group `ingestion`, kinds
   `python`/`databricks`) — calls `extract_posts()` and returns `rows_written`.
2. **`reddit_raw/r_comments`** (Python) — calls `extract_comments()`; depends on the
   posts load, which it derives from.
3. **`reddit_raw/r_users`** (Python) — calls `extract_users()`; depends on the posts
   load for the same reason.
4. **`dbt_reddit`** (dbt) — one `dbt build` (staging → intermediate → marts → tests)
   through `DbtCliResource`, yielding the eight dbt assets.

Each extractor asset is declared under the asset key Dagster derives for its dbt
source (`<source_name>/<table_name>`, e.g. `reddit_raw/r_worldnews`), so the two
definitions merge and the extractor becomes the upstream of `stg_reddit_posts`. That
merge is what makes one run extract and then build instead of doing either half
alone.

All four assets — the three extractors and `dbt_reddit` — carry the ported Prefect
retry policy: three retries on an exponential backoff with jitter.

## Assets, checks, job, schedules

| Kind | Count | Names |
| --- | --- | --- |
| Assets | 11 | `reddit_raw/r_worldnews`, `reddit_raw/r_comments`, `reddit_raw/r_users`, `stg_reddit_posts`, `stg_reddit_comments`, `stg_reddit_users`, `int_posts_enriched`, `int_comments_enriched`, `mart_post_summary`, `mart_user_metrics`, `mart_comment_history` |
| Asset checks | 28 | every dbt test declared on the eight models |
| Asset groups | 2 | `ingestion` (the extractors), `transformation` (the eight dbt assets) |
| Job | 1 | `reddit_refresh` = `AssetSelection.all()` |
| Schedules | 0 | none — Prefect was triggered on demand, and the port keeps it that way |

Groups are assigned in `definitions.py`: `group_name="ingestion"` on the extractor
assets, and `RedditDbtTranslator.get_group_name()` returning `transformation` for
every dbt node. The translator deliberately returns `None` for dbt *sources*,
because each `reddit_raw.r_*` source shares its asset key with an extractor and a
second group on that key would conflict with `ingestion`. Both groups are selectable:
`dg.AssetSelection.groups("ingestion")` / `.groups("transformation")`.

`enable_asset_checks=True` turns the 28 model tests into checks on their models.
`enable_source_tests_as_checks=True` is set so that any test later declared on the
`reddit_raw` source attaches to its extractor rather than being silently dropped.

## Commands

Run from `orchestration/`:

```bash
# UI plus scheduler/daemon; code location auto-detected from [tool.dagster] module_name.
dagster dev

# Headless: extract, then dbt build, once. No UI. `dagster dev` is the command that
# reads [tool.dagster]; `asset materialize` takes a workspace target, so name the
# module with -m (it works from any directory once the package is installed).
dagster asset materialize -m reddit_pipeline_orchestration.definitions --select '*'
```

`asset materialize` prints Dagster's first-run telemetry banner to stdout. Silence it
by creating a `$DAGSTER_HOME/dagster.yaml` containing `telemetry: {enabled: false}`.

## Environment

The code location reads the same `.env` as the extractors: `REDDIT_CLIENT_ID`,
`REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`, `DATABRICKS_HOSTNAME`,
`DATABRICKS_HTTP_PATH`, the optional `DATABRICKS_CONFIG_PROFILE` and the optional
`LOG_LEVEL`. Those values must be present **when the code location loads**, not only
when dbt runs: `profiles/profiles.yml` resolves them through `env_var(...)` while the
dbt manifest is parsed, so a missing host or path fails at load time.

No token belongs in the environment. Both halves authenticate from the CLI profile in
`~/.databrickscfg` (`databricks auth login`): the extractors hand the SQL connector a
credentials provider, and the dbt asset mints a token from the same profile at the
start of each run, immediately before invoking `dbt`. A token exported into a shell
dies after an hour, which would leave every run on a long-lived code server failing at
session open with `RequestError: refresh the databricks token`.

dbt does not read `.env` — only the extractors' `load_dotenv()` does — so to run
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
python -m ingestion.reddit.extract_reddit
python -m ingestion.reddit.extract_reddit_comments
python -m ingestion.reddit.extract_reddit_users

# 2. Build and test the models against the committed profile.
DBT_PROFILES_DIR=profiles dbt build
```

## Tests

`pytest` stays the offline pre-merge gate — no network, no warehouse. `conftest.py`
parses the dbt project once per session (dummy `DATABRICKS_*` values are enough, and
it sets them itself) so the code location is imported against a manifest that matches
the checked-out models, never against whatever `target/` happened to hold. dbt's
tests need a warehouse, so they run as part of the orchestrated `dbt build` (or by
hand with `dbt build`).

Two modules in the offline suite cover this layer:

- `tests/test_definitions.py`: the code location's wiring — the asset set, the
  extractor→staging merges, the retry policy, all 28 asset checks, the job's
  selection, and the absence of a schedule.
- `tests/test_extract_assets.py`: each extractor asset — the `rows_written`
  materialization metadata and failure propagation.
