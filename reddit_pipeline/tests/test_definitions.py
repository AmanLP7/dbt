"""The Dagster job that ties the extractors to the dbt models.

`definitions.py` is the only place the two halves of the pipeline meet: each Python
asset carries the key Dagster derives for its `reddit_raw.r_*` dbt source, so the source
specs merge into the extractors and the staging models depend on the loads. Nothing else
in the repo pins that merge, the 28 model tests the translator turns into asset checks, or
the job's selection, so a renamed key or a flipped translator default could cut the
extractors out of a run while every other test still passed. These assertions are
offline: they read the resolved graph, never a warehouse or the network.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from pathlib import Path

import dagster as dg
import pytest
from reddit_pipeline_orchestration import definitions as d

POSTS = d.RAW_POSTS_KEY
COMMENTS = d.RAW_COMMENTS_KEY
USERS = d.RAW_USERS_KEY
# The `source/table` strings the Dagster UI shows, and the keys the dbt sources merge into.
POSTS_KEY, COMMENTS_KEY, USERS_KEY = (key.to_user_string() for key in (POSTS, COMMENTS, USERS))
SPECS: dict[str, dg.AssetSpec] = {spec.key.to_user_string(): spec for spec in d.defs.resolve_all_asset_specs()}
GRAPH = d.defs.resolve_asset_graph()

EXTRACTOR_KEYS = {POSTS_KEY, COMMENTS_KEY, USERS_KEY}
MODEL_KEYS = {
    "stg_reddit_posts",
    "stg_reddit_comments",
    "stg_reddit_users",
    "int_posts_enriched",
    "int_comments_enriched",
    "mart_post_summary",
    "mart_user_metrics",
    "mart_comment_history",
}
ASSET_KEYS = EXTRACTOR_KEYS | MODEL_KEYS


def user_keys(asset_keys: Iterable[dg.AssetKey]) -> set[str]:
    """The `source/table` strings the Dagster UI shows, so assertions read as the graph does."""
    return {key.to_user_string() for key in asset_keys}


def dep_keys(spec: dg.AssetSpec) -> set[str]:
    return user_keys(dep.asset_key for dep in spec.deps)


def test_the_pipeline_is_three_python_extractors_and_eight_dbt_models() -> None:
    assert set(SPECS) == ASSET_KEYS

    for key in EXTRACTOR_KEYS:
        assert SPECS[key].group_name == "ingestion", key
        assert SPECS[key].kinds >= {"python", "databricks"}, key

    for key in MODEL_KEYS:
        assert SPECS[key].kinds >= {"dbt", "databricks"}, key
        assert SPECS[key].group_name == "transformation", key


def test_the_extractors_land_the_dbt_sources_and_the_marts_chain_off_staging() -> None:
    assert dep_keys(SPECS[POSTS_KEY]) == set(), "the posts extractor is a source, not a consumer"
    # Comments and users derive from the same hot posts, so both follow the posts load.
    assert dep_keys(SPECS[COMMENTS_KEY]) == {POSTS_KEY}
    assert dep_keys(SPECS[USERS_KEY]) == {POSTS_KEY}

    assert dep_keys(SPECS["stg_reddit_posts"]) == {POSTS_KEY}
    assert dep_keys(SPECS["stg_reddit_comments"]) == {COMMENTS_KEY}
    assert dep_keys(SPECS["stg_reddit_users"]) == {USERS_KEY}

    assert dep_keys(SPECS["int_posts_enriched"]) == {"stg_reddit_posts", "stg_reddit_users"}
    assert dep_keys(SPECS["int_comments_enriched"]) == {
        "stg_reddit_comments",
        "stg_reddit_posts",
        "stg_reddit_users",
    }
    assert dep_keys(SPECS["mart_post_summary"]) == {"int_posts_enriched", "stg_reddit_comments"}
    assert dep_keys(SPECS["mart_user_metrics"]) == {
        "stg_reddit_posts",
        "stg_reddit_comments",
        "stg_reddit_users",
    }
    assert dep_keys(SPECS["mart_comment_history"]) == {"int_comments_enriched", "stg_reddit_posts"}


def test_every_extractor_is_an_ancestor_of_the_comment_history_mart() -> None:
    # The job's selection is everything, but even a one-asset run has to start by
    # landing all three raw tables: the mart reads posts, comments and users.
    ancestors = user_keys(GRAPH.get_ancestor_asset_keys(dg.AssetKey(["mart_comment_history"])))
    assert ancestors >= EXTRACTOR_KEYS
    assert "mart_comment_history" not in ancestors


def test_the_twenty_eight_model_tests_become_asset_checks() -> None:
    assert len(GRAPH.asset_check_keys) == 28

    # The source declarations carry no tests, so no check attaches to an extractor.
    for key in (POSTS, COMMENTS, USERS):
        assert GRAPH.get_check_keys_for_assets({key}) == set(), key


def test_the_job_selects_every_asset() -> None:
    assert user_keys(d.reddit_refresh.selection.resolve(GRAPH)) == ASSET_KEYS


def test_no_schedule_ships_with_the_port() -> None:
    # Prefect was triggered on demand, so the port keeps that: no schedule until a
    # cadence is decided, and nothing writes to the warehouse by itself.
    assert list(d.defs.schedules or []) == []


def test_the_python_and_dbt_steps_carry_a_retry_policy() -> None:
    # A transient Reddit, warehouse or dbt failure should not need a manual re-run; the
    # policy now lives on the assets instead of the Prefect task decorators. Dagster
    # keeps it on the op definition, which is where the run looks for it.
    for asset in (d.raw_reddit_posts, d.raw_reddit_comments, d.raw_reddit_users, d.dbt_reddit):
        policy = asset.op.retry_policy
        assert policy is not None, asset.op.name
        assert policy.max_retries == 3, asset.op.name
        assert policy.delay == 10, asset.op.name
        assert policy.backoff == dg.Backoff.EXPONENTIAL, asset.op.name
        assert policy.jitter == dg.Jitter.PLUS_MINUS, asset.op.name


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
    """CI has no `~/.databrickscfg`; the token it was given must still be what dbt gets,
    and the fall-through must not be silent.

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


def test_a_malformed_cli_config_does_not_fail_the_code_location(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """A truncated `~/.databrickscfg` must not take the code location down on import.

    The SDK parses that file itself and reports syntax errors as `configparser.Error`,
    which is neither a `ValueError` nor an `OSError`, so it has to be named explicitly.
    """
    broken = tmp_path / "databrickscfg"
    broken.write_text("[DEFAULT\nhost = x\n", encoding="utf-8")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-supplied")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(broken))
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)

    with caplog.at_level(logging.WARNING, logger=d.__name__):
        d.refresh_dbt_token()

    assert os.environ["DATABRICKS_TOKEN"] == "dapi-supplied"
    assert "Keeping the DATABRICKS_TOKEN from the environment" in caplog.text
