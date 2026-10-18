"""Contract of the Dagster wrapper around the extractor.

`raw_job_postings` is a thin adapter: call `extract_job_postings()` once, publish
its row count as the `rows_written` materialization metadata a Dagster user reads
in the UI, and let a failure escape so the run fails instead of looking like a
successful zero-row fetch. The extractor is replaced at the definitions module
attribute, so neither the three job boards nor the warehouse are touched here.
"""

from __future__ import annotations

import dagster as dg
import pytest
from job_postings_orchestration import definitions as d

ROWS_WRITTEN = 4242


def test_asset_materializes_the_extractor_row_count(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_extract() -> int:
        nonlocal calls
        calls += 1
        return ROWS_WRITTEN

    monkeypatch.setattr(d, "extract_job_postings", fake_extract)

    result = dg.materialize([d.raw_job_postings])

    assert result.success is True
    assert calls == 1, "the asset calls the extractor exactly once per materialization"
    materializations = result.asset_materializations_for_node(d.raw_job_postings.node_def.name)
    assert len(materializations) == 1
    assert materializations[0].metadata["rows_written"].value == ROWS_WRITTEN


def test_extractor_failure_fails_the_run_without_materializing(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_extract() -> int:
        raise RuntimeError("all sources failed")

    monkeypatch.setattr(d, "extract_job_postings", failing_extract)

    result = dg.materialize([d.raw_job_postings], raise_on_error=False)

    assert result.success is False
    assert result.get_asset_materialization_events() == [], "a failed extract has no row count to record"
