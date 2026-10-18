"""Contracts of the Dagster wrappers around the three extractors.

Each `raw_reddit_*` asset is a thin adapter: call its entrypoint once, publish the
returned row count as the `rows_written` materialization metadata a Dagster user reads
in the UI, and let a failure escape so the run fails instead of looking like a
successful zero-row fetch. The entrypoints are replaced at the definitions module
attribute, so neither Reddit nor the warehouse is touched here.
"""

from __future__ import annotations

import dagster as dg
import pytest
from reddit_pipeline_orchestration import definitions as d

ROWS_WRITTEN = 4242

EXTRACTORS = [
    (d.raw_reddit_posts, "extract_posts"),
    (d.raw_reddit_comments, "extract_comments"),
    (d.raw_reddit_users, "extract_users"),
]


@pytest.mark.parametrize(("asset", "entrypoint"), EXTRACTORS, ids=[name for _, name in EXTRACTORS])
def test_asset_materializes_the_extractor_row_count(
    asset: dg.AssetsDefinition,
    entrypoint: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_extract() -> int:
        nonlocal calls
        calls += 1
        return ROWS_WRITTEN

    monkeypatch.setattr(d, entrypoint, fake_extract)

    result = dg.materialize([asset])

    assert result.success is True
    assert calls == 1, "the asset calls the extractor exactly once per materialization"
    materializations = result.asset_materializations_for_node(asset.node_def.name)
    assert len(materializations) == 1
    assert materializations[0].metadata["rows_written"].value == ROWS_WRITTEN


@pytest.mark.parametrize(("asset", "entrypoint"), EXTRACTORS, ids=[name for _, name in EXTRACTORS])
def test_extractor_failure_fails_the_run_without_materializing(
    asset: dg.AssetsDefinition,
    entrypoint: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The asset's real policy backs off 10s per retry; with three of them the failure
    # path alone would add minutes to an offline suite. Retry behaviour is asserted
    # separately in test_definitions.py, so this test only needs the terminal state.
    monkeypatch.setattr(asset.op, "_retry_policy", dg.RetryPolicy(max_retries=0))

    def failing_extract() -> int:
        raise RuntimeError("reddit api down")

    monkeypatch.setattr(d, entrypoint, failing_extract)

    result = dg.materialize([asset], raise_on_error=False)

    assert result.success is False
    assert result.get_asset_materialization_events() == [], "a failed extract has no row count to record"
