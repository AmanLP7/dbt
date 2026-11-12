"""Dagster definitions for the Reddit pipeline.

Four tasks, in dependency order:

1. ``raw_reddit_posts`` (Python) — fetch the new and top-of-day posts of every
   subreddit in ``seeds/sections.csv`` into ``workspace.topic_raw.r_posts``.
2. ``raw_reddit_comments`` (Python) — fetch the top comments of the most-commented
   in-window posts into ``workspace.topic_raw.r_comments``.
3. ``raw_reddit_users`` (Python) — fetch post/comment authors into
   ``workspace.topic_raw.r_users``.
4. ``dbt_reddit`` (dbt) — one ``dbt build``: staging, intermediate, marts and their
   tests.

The job runs on demand and, once someone starts it in the UI, on
``daily_edition_refresh`` (05:00 UTC). The ``daily`` mart layer it feeds is
incremental on ``edition_date``, so each day's paper is written once and every older
edition stays readable after the raw tables roll.

Each extractor asset carries the asset key Dagster derives for its dbt source
(``<source_name>/<table_name>``), so the Python asset and the dbt source spec merge:
the extractors become the upstreams of the staging models and a run materializes
the loads first.
"""

# No `from __future__ import annotations` here, unlike the rest of the project:
# dagster compares the `context` parameter's annotation *object*, so the postponed
# string form is rejected at decoration time ("Cannot annotate `context` parameter
# with type dg.AssetExecutionContext"). Imported classes are used directly instead.
import configparser
import logging
import os
import sys
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

# The repo root *is* the dbt project, so the orchestration package sits beside the
# models it schedules: <repo>/orchestration/src/reddit_pipeline_orchestration/definitions.py
#
# `__init__.py` does this for package imports, but `definitions.py` is also loaded by
# path -- `dagster-dbt project prepare-and-package --file ...`, and pytest -- where
# `__init__.py` never runs. Without this, `ingestion` is not importable in those modes.
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion.reddit._common import databricks_token  # noqa: E402
from ingestion.reddit.extract_reddit import extract_posts  # noqa: E402
from ingestion.reddit.extract_reddit_comments import extract_comments  # noqa: E402
from ingestion.reddit.extract_reddit_users import extract_users  # noqa: E402

LOGGER = logging.getLogger(__name__)

DBT_PROJECT = DbtProject(
    project_dir=REPO_ROOT,
    # profiles/profiles.yml is committed, and it reads the same DATABRICKS_* variables
    # the extractors do, so an orchestrated run and a hand-run `dbt build` cannot
    # disagree about which workspace they write to. dagster-dbt exports
    # DBT_PROFILES_DIR to every dbt subprocess, so ~/.dbt/profiles.yml is not consulted.
    profiles_dir=REPO_ROOT / "profiles",
    profile="reddit_pipeline",
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
    `OSError` covers the SDK shelling out to a `databricks` CLI that is not installed,
    and `configparser.Error` a truncated or hand-edited `~/.databrickscfg`, which the
    SDK parses itself. Any of these must leave the code location loadable rather than
    taking it down. The environment token is then left untouched, and logging says so,
    because a silent fall-through to a stale token is the failure this function exists
    to prevent.
    """
    try:
        os.environ["DATABRICKS_TOKEN"] = databricks_token()
    except (configparser.Error, ValueError, OSError) as error:
        LOGGER.warning("Keeping the DATABRICKS_TOKEN from the environment: %s", error)


# In development this regenerates target/manifest.json while the definitions load, so
# the asset graph always matches the checked-out models. Deployment instead
# precompiles it once:
#   dagster-dbt project prepare-and-package --file orchestration/src/reddit_pipeline_orchestration/definitions.py
refresh_dbt_token()
DBT_PROJECT.prepare_if_dev()
if not DBT_PROJECT.manifest_path.exists() and DBT_PROJECT.preparer is not None:
    # `prepare_if_dev` is a no-op outside `dagster dev`, so a first import on a fresh
    # checkout -- the precompile command above, or an interpreter that never ran
    # `dbt parse` -- has no manifest yet, and the decorator below reads it immediately.
    # Parsing here costs nothing once target/ exists.
    DBT_PROJECT.preparer.prepare(DBT_PROJECT)

RAW_POSTS_KEY = dg.AssetKey(["reddit_raw", "r_posts"])
RAW_COMMENTS_KEY = dg.AssetKey(["reddit_raw", "r_comments"])
RAW_USERS_KEY = dg.AssetKey(["reddit_raw", "r_users"])


# dbt's tests become Dagster asset checks: every test declared on a model or a seed
# attaches to that node automatically. Source tests, if any are added to src_reddit.yml,
# attach to the extractor assets only when `enable_source_tests_as_checks` is set, which
# is what makes the extractor's `source` values a checked contract rather than a guess.
#
# The group is set here rather than in dbt_project.yml because dbt's own `group:`
# config requires a matching `groups:` entry in a dbt-side YAML file, which would add
# a dbt concept to the project purely to relabel Dagster's UI.
class RedditDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Group every dbt-built asset as `transformation`.

        Sources return None, meaning Dagster's default group: each
        `reddit_raw.r_*` source shares its asset key with an extractor, and a
        second, conflicting group on that key would be a definition error.
        """
        if dbt_resource_props["resource_type"] == "source":
            return None
        return "transformation"


DBT_TRANSLATOR = RedditDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_asset_checks=True,
        enable_source_tests_as_checks=True,
    )
)

# The three loads and the dbt build used to run as Prefect tasks that retried three
# times. Dagster's RetryPolicy cannot express the exact 10s/60s/300s ladder, so it
# keeps the retry count and uses an exponential backoff with jitter instead.
RETRY_POLICY = dg.RetryPolicy(
    max_retries=3,
    delay=10,
    backoff=dg.Backoff.EXPONENTIAL,
    jitter=dg.Jitter.PLUS_MINUS,
)


@dg.asset(
    key=RAW_POSTS_KEY,
    group_name="ingestion",
    kinds={"python", "databricks"},
    retry_policy=RETRY_POLICY,
    description=(
        "The new and top-of-day listings of every seeded subreddit, filtered to the "
        "26-hour read window and upserted by post id into workspace.topic_raw.r_posts. "
        "Comments and users load from the same posts, so this runs first."
    ),
)
def raw_reddit_posts(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Task 1: fetch every seeded subreddit's listings and upsert the posts."""
    rows_written = extract_posts()
    context.log.info("Wrote %s posts", rows_written)
    return dg.MaterializeResult(metadata={"rows_written": rows_written})


@dg.asset(
    key=RAW_COMMENTS_KEY,
    deps=[RAW_POSTS_KEY],
    group_name="ingestion",
    kinds={"python", "databricks"},
    retry_policy=RETRY_POLICY,
    description=(
        "Top comments of the 200 most-commented in-window posts, upserted by comment id "
        "into workspace.topic_raw.r_comments."
    ),
)
def raw_reddit_comments(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Task 2: fetch the most-commented posts' comments and upsert them."""
    rows_written = extract_comments()
    context.log.info("Wrote %s comments", rows_written)
    return dg.MaterializeResult(metadata={"rows_written": rows_written})


@dg.asset(
    key=RAW_USERS_KEY,
    deps=[RAW_POSTS_KEY],
    group_name="ingestion",
    kinds={"python", "databricks"},
    retry_policy=RETRY_POLICY,
    description=(
        "Post and comment authors as key-based upserts in workspace.topic_raw.r_users, "
        "so returning users receive refreshed karma and new users are appended."
    ),
)
def raw_reddit_users(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Task 3: collect the post and comment authors across the seed and upsert them."""
    rows_written = extract_users()
    context.log.info("Wrote %s users", rows_written)
    return dg.MaterializeResult(metadata={"rows_written": rows_written})


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    dagster_dbt_translator=DBT_TRANSLATOR,
    retry_policy=RETRY_POLICY,
)
def dbt_reddit(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Task 4: build and test every dbt model in one `dbt build`, via the dbt CLI."""
    # profiles/profiles.yml hands dbt a token and DbtCliResource copies os.environ into
    # the dbt subprocess (it takes no env of its own). dbt itself cannot accept a
    # credentials provider, so the token is minted here, per run.
    refresh_dbt_token()
    yield from dbt.cli(["build"], context=context).stream()


reddit_refresh = dg.define_asset_job(
    name="reddit_refresh",
    selection=dg.AssetSelection.all(),
    description="Land the posts, comments and users, then rebuild and test the models.",
)

# The edition runs once a day, not on every extractor run: the daily marts are
# incremental on `edition_date`, so a second build inside the same day replaces that
# day's page rather than adding to it, and the app reads one stable snapshot.
# 05:00 UTC is after the American evening and before the European morning, so the
# widest set of the previous day's stories has been posted by the time it builds.
# Created stopped, matching `daily_job_board_refresh` in the sibling project: a
# deployment does not write to the warehouse until someone flips it in the UI.
daily_edition_refresh = dg.ScheduleDefinition(
    name="daily_edition_refresh",
    job=reddit_refresh,
    cron_schedule="0 5 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[raw_reddit_posts, raw_reddit_comments, raw_reddit_users, dbt_reddit],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT)},
    jobs=[reddit_refresh],
    schedules=[daily_edition_refresh],
)
