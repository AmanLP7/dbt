"""The shape of the Dagster job that ties the extractor to the dbt models.

`definitions.py` is the only place the two halves of the pipeline meet: the Python asset
carries the key Dagster derives for the `jobs.raw_job_postings` dbt source, so the source
spec merges into it and `stg_job_postings` depends on the fetch. Nothing else in the repo
pins that merge, the four source tests the translator turns into asset checks, or the
schedule shipping stopped, so a renamed key or flipped translator default could cut the
extractor out of a run while every other test still passed. These assertions are offline:
they read the resolved graph, never a warehouse or the network.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable

import dagster as dg
import pytest
from job_postings_orchestration import definitions as d

EXTRACTOR = d.RAW_JOB_POSTINGS_KEY
# The key Dagster shows for the extractor, and the one the dbt source merges into.
EXTRACTOR_KEY = EXTRACTOR.to_user_string()
SPECS: dict[str, dg.AssetSpec] = {spec.key.to_user_string(): spec for spec in d.defs.resolve_all_asset_specs()}
GRAPH = d.defs.resolve_asset_graph()

# The extractor plus the seed and the three models `dbt build` also materializes.
ASSET_KEYS = {
    EXTRACTOR_KEY,
    "role_keywords",
    "stg_job_postings",
    "mart_role_listings",
    "mart_role_summary",
}
MODEL_KEYS = ASSET_KEYS - {EXTRACTOR_KEY}
SOURCE_CHECKS = {
    "source_accepted_values_jobs_raw_job_postings_source__remotive__arbeitnow__jobicy",
    "source_not_null_jobs_raw_job_postings_ingested_at",
    "source_not_null_jobs_raw_job_postings_source",
    "source_not_null_jobs_raw_job_postings_source_job_id",
}


def user_keys(asset_keys: Iterable[dg.AssetKey]) -> set[str]:
    """The `source/table` strings the Dagster UI shows, so assertions read as the graph does."""
    return {key.to_user_string() for key in asset_keys}


def dep_keys(spec: dg.AssetSpec) -> set[str]:
    return user_keys(dep.asset_key for dep in spec.deps)


def test_the_pipeline_is_one_python_extractor_and_four_dbt_models() -> None:
    assert set(SPECS) == ASSET_KEYS

    extractor = SPECS["jobs/raw_job_postings"]
    assert extractor.group_name == "ingestion"
    assert extractor.kinds >= {"python", "databricks"}

    for key in MODEL_KEYS:
        assert SPECS[key].kinds >= {"dbt", "databricks"}, key
        assert SPECS[key].group_name == "transformation", key


def test_the_extractor_lands_the_dbt_source_and_the_marts_chain_off_staging() -> None:
    assert dep_keys(SPECS["jobs/raw_job_postings"]) == set(), "the extractor is a source, not a consumer"
    assert dep_keys(SPECS["stg_job_postings"]) == {"jobs/raw_job_postings"}
    assert dep_keys(SPECS["mart_role_listings"]) == {"stg_job_postings", "role_keywords"}
    assert dep_keys(SPECS["mart_role_summary"]) == {"mart_role_listings"}


def test_the_extractor_is_an_ancestor_of_the_summary_mart() -> None:
    ancestors = user_keys(GRAPH.get_ancestor_asset_keys(dg.AssetKey(["mart_role_summary"])))
    assert ancestors == ASSET_KEYS - {"mart_role_summary"}


def test_the_ten_model_tests_and_four_source_tests_become_asset_checks() -> None:
    assert len(GRAPH.asset_check_keys) == 14

    attached = GRAPH.get_check_keys_for_assets({EXTRACTOR})
    assert {check.name for check in attached} == SOURCE_CHECKS


def test_the_job_selects_every_asset() -> None:
    assert user_keys(d.job_board_refresh.selection.resolve(GRAPH)) == ASSET_KEYS


def test_the_schedule_ships_stopped() -> None:
    # `defs.schedules` is a plain iterable of both schedule flavours, so materialise it.
    schedules = list(d.defs.schedules or [])
    assert len(schedules) == 1
    schedule = schedules[0]
    assert isinstance(schedule, dg.ScheduleDefinition)
    assert schedule.name == "daily_job_board_refresh"
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.job_name == "job_board_refresh"
    assert schedule.default_status is dg.DefaultScheduleStatus.STOPPED


def test_a_cli_profile_token_is_published_for_dbt(monkeypatch: pytest.MonkeyPatch) -> None:
    """dbt reads DATABRICKS_TOKEN through `env_var`, so the mint has to reach the environment.

    The variable starts out holding the stale token a long-lived process captured at
    startup, which is precisely the value that must not survive into `dbt build`.
    """
    monkeypatch.setattr(d, "databricks_token", lambda: "token-from-the-cli-profile")
    monkeypatch.setenv("DATABRICKS_TOKEN", "stale-token-captured-at-startup")

    d.refresh_dbt_token()

    assert os.environ["DATABRICKS_TOKEN"] == "token-from-the-cli-profile"


def test_an_environment_token_survives_without_a_cli_profile(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """CI and a deployed code server have no `~/.databrickscfg`; the token they were given
    must still be what dbt gets, and the fall-through must not be silent.

    This drives the real `databricks_token()` rather than stubbing the exception, so it
    holds only while the SDK reports a missing profile as something `refresh_dbt_token()`
    catches. Pointing `DATABRICKS_CONFIG_FILE` at a nonexistent file is what makes the
    test independent of the developer's own `~/.databrickscfg`.
    """
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-supplied")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", "/nonexistent/databrickscfg")
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)

    with caplog.at_level(logging.WARNING, logger=d.__name__):
        d.refresh_dbt_token()

    assert os.environ["DATABRICKS_TOKEN"] == "dapi-supplied"
    assert "Keeping the DATABRICKS_TOKEN from the environment" in caplog.text


def test_a_profile_that_cannot_mint_a_token_is_reported(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A named profile that cannot mint is a misconfiguration, and it must be reported
    rather than passed over: silence here is what makes a stale token look healthy."""
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "no-such-profile")
    monkeypatch.setenv("DATABRICKS_TOKEN", "stale-token-captured-at-startup")

    with caplog.at_level(logging.WARNING, logger=d.__name__):
        d.refresh_dbt_token()

    assert os.environ["DATABRICKS_TOKEN"] == "stale-token-captured-at-startup"
    assert "Keeping the DATABRICKS_TOKEN from the environment" in caplog.text
