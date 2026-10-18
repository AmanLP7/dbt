"""Dagster definitions for the job-postings pipeline.

Two tasks, in dependency order:

1. ``raw_job_postings`` (Python) — fetch the three keyless boards and append the
   rows to the Databricks Delta table.
2. ``dbt_job_postings`` (dbt) — one ``dbt build``: the seed, then staging, then the
   marts, each followed by its tests.

``raw_job_postings`` carries the asset key Dagster derives for the dbt source
``jobs.raw_job_postings`` (``<source_name>/<table_name>``), so the Python asset and
the dbt source spec merge: the extractor becomes the upstream of ``stg_job_postings``
and a run materializes the extractor first.
"""

# No `from __future__ import annotations` here, unlike the rest of the project:
# dagster compares the `context` parameter's annotation *object*, so the postponed
# string form is rejected at decoration time ("Cannot annotate `context` parameter
# with type dg.AssetExecutionContext"). Imported classes are used directly instead.
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from ingestion.jobs._common import databricks_token
from ingestion.jobs.fetch_job_postings import extract_job_postings

LOGGER = logging.getLogger(__name__)

# The repo root *is* the dbt project, so the orchestration package sits beside the
# models it schedules: <repo>/orchestration/src/job_postings_orchestration/definitions.py
REPO_ROOT = Path(__file__).resolve().parents[3]

DBT_PROJECT = DbtProject(
    project_dir=REPO_ROOT,
    # profiles/profiles.yml is committed, and it reads the same DATABRICKS_* variables
    # the extractor does, so an orchestrated run and a hand-run `dbt build` cannot
    # disagree about which workspace they write to. dagster-dbt exports
    # DBT_PROFILES_DIR to every dbt subprocess, so ~/.dbt/profiles.yml is not consulted.
    profiles_dir=REPO_ROOT / "profiles",
    profile="job_postings",
    target="dev",
)


def refresh_dbt_token() -> None:
    """Put a live token in the environment for dbt, taken from the Databricks CLI profile.

    `profiles/profiles.yml` resolves `DATABRICKS_TOKEN` through `env_var(...)`, and dbt
    has no way to accept a credentials provider, so the variable must hold a valid token
    at each point dbt runs: the `dbt parse` that `prepare_if_dev()` triggers at
    code-location load in development, and the `dbt build` the asset runs. Refreshing at
    those points is what stops a code server that has been up for hours from handing dbt
    a token that expired an hour after it started.

    `Config(profile=...)` states both "no profile is configured" and "the requested one
    is not there" as `ValueError`; either way there is nothing to mint, and CI — which
    supplies `DATABRICKS_TOKEN` itself and has no `~/.databrickscfg` — takes that path.
    `OSError` covers the SDK shelling out to a `databricks` CLI that is not installed.
    The environment token is then left untouched, and logging says so, because a silent
    fall-through to a stale token is the failure this function exists to prevent.
    """
    try:
        os.environ["DATABRICKS_TOKEN"] = databricks_token()
    except (ValueError, OSError) as error:
        LOGGER.warning("Keeping the DATABRICKS_TOKEN from the environment: %s", error)


# In development this regenerates target/manifest.json while the definitions load, so
# the asset graph always matches the checked-out models. Deployment instead
# precompiles it once:
#   dagster-dbt project prepare-and-package --file orchestration/src/job_postings_orchestration/definitions.py
refresh_dbt_token()
DBT_PROJECT.prepare_if_dev()

RAW_JOB_POSTINGS_KEY = dg.AssetKey(["jobs", "raw_job_postings"])


# dbt's tests become Dagster asset checks: the ten declared on models attach to their
# model automatically. The four declared on the `jobs.raw_job_postings` source only
# attach to the extractor asset when `enable_source_tests_as_checks` is set, which is
# what makes the extractor's `source` values a checked contract rather than a guess.
#
# The group is set here rather than in dbt_project.yml because dbt's own `group:`
# config requires a matching `groups:` entry in a dbt-side YAML file, which would add
# a dbt concept to the project purely to relabel Dagster's UI.
class JobPostingsDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Group every dbt-built asset (seed and models) as `transformation`.

        Sources return None, meaning Dagster's default group: the
        `jobs.raw_job_postings` source shares its asset key with the extractor, and a
        second, conflicting group on that key would be a definition error.
        """
        if dbt_resource_props["resource_type"] == "source":
            return None
        return "transformation"


DBT_TRANSLATOR = JobPostingsDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_asset_checks=True,
        enable_source_tests_as_checks=True,
    )
)


@dg.asset(
    key=RAW_JOB_POSTINGS_KEY,
    group_name="ingestion",
    kinds={"python", "databricks"},
    description=(
        "Append-only landing table: one row per listing per fetch from remotive, "
        "arbeitnow and jobicy. A source that fails is skipped; the run aborts only "
        "if all three fail."
    ),
)
def raw_job_postings(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Task 1: fetch every board and append the normalised rows to raw."""
    rows_written = extract_job_postings()
    context.log.info("Appended %s postings", rows_written)
    return dg.MaterializeResult(metadata={"rows_written": rows_written})


@dbt_assets(manifest=DBT_PROJECT.manifest_path, dagster_dbt_translator=DBT_TRANSLATOR)
def dbt_job_postings(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Task 2: seed, models and tests in one `dbt build`, driven by the dbt CLI."""
    # profiles/profiles.yml hands dbt a token and DbtCliResource copies os.environ into
    # the dbt subprocess (it takes no env of its own). dbt itself cannot accept a
    # credentials provider, so the token is minted here, per run.
    refresh_dbt_token()
    yield from dbt.cli(["build"], context=context).stream()


job_board_refresh = dg.define_asset_job(
    name="job_board_refresh",
    selection=dg.AssetSelection.all(),
    description="Land a fresh fetch, then rebuild and test the models.",
)

# Created stopped: Dagster's default. A deployment does not start writing to the
# warehouse until someone flips it in the UI (or sets RUNNING here).
daily_job_board_refresh = dg.ScheduleDefinition(
    name="daily_job_board_refresh",
    job=job_board_refresh,
    cron_schedule="0 6 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[raw_job_postings, dbt_job_postings],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT)},
    jobs=[job_board_refresh],
    schedules=[daily_job_board_refresh],
)
