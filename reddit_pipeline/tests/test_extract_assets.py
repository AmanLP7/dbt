"""Contracts of the Dagster wrappers around the extractors.

Each `raw_reddit_*` asset is a thin adapter: call its entrypoint once, publish the
returned row count as the `rows_written` materialization metadata, land the dump with
one `copy_into_raw` call for its own table, and let a failure escape so the run fails
instead of looking like a successful zero-row fetch. The entrypoints and
`copy_into_raw` are replaced at the definitions module attribute and the SQLite
resource is faked, so neither Reddit nor Databricks is touched here.
"""

from __future__ import annotations

import dagster as dg
import pytest
from reddit_pipeline_orchestration import definitions as d

ROWS_WRITTEN = 4242

# Every load takes the run's shared SQLite connection; there is no per-subreddit
# split, so each raw asset fetches the whole seed once.
EXTRACTORS = [
    (d.raw_reddit_posts, "extract_posts", None),
    (d.raw_reddit_comments, "extract_comments", None),
    (d.raw_reddit_users, "extract_users", None),
]
IDS = ["posts", "comments", "users"]

# The three raw assets each land their own table with one `copy_into_raw` call after
# fetching their rows into the shared SQLite dump.
COPY_TABLES = [
    (d.raw_reddit_posts, "r_posts"),
    (d.raw_reddit_comments, "r_comments"),
    (d.raw_reddit_users, "r_users"),
]
COPY_IDS = ["posts", "comments", "users"]

CONN = object()


class FakeSqlite(dg.ConfigurableResource):
    """Stands in for `d.RedditSqlite`, whose setup would open a SQLite connection."""

    @property
    def connection(self) -> object:
        return CONN


class FakeCopyResult:
    """Stands in for `ingestion.reddit.copy_into.CopyResult`; only `.rows` is read."""

    def __init__(self, rows: dict[str, int]) -> None:
        self.rows = rows


def resources() -> dict[str, dg.ConfigurableResource]:
    return {"sqlite": FakeSqlite()}


@pytest.mark.parametrize(("asset", "entrypoint", "subreddit"), EXTRACTORS, ids=IDS)
def test_asset_materializes_the_extractor_row_count(
    asset: dg.AssetsDefinition,
    entrypoint: str,
    subreddit: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    def fake_extract(*args: object) -> int:
        calls.append(args)
        return ROWS_WRITTEN

    monkeypatch.setattr(d, entrypoint, fake_extract)
    # Every raw asset also lands its dump after fetching; stub that out so only the
    # extractor call is what this test observes.
    monkeypatch.setattr(d, "copy_into_raw", lambda **kwargs: FakeCopyResult({}))

    result = dg.materialize([asset], resources=resources())

    assert result.success is True
    assert len(calls) == 1, "the asset calls the extractor exactly once per materialization"
    assert calls[0] == (CONN,), "the load runs on the run's shared SQLite connection"
    materializations = result.asset_materializations_for_node(asset.node_def.name)
    assert len(materializations) == 1
    assert materializations[0].metadata["rows_written"].value == ROWS_WRITTEN


@pytest.mark.parametrize(("asset", "entrypoint", "subreddit"), EXTRACTORS, ids=IDS)
def test_extractor_failure_fails_the_run_without_materializing(
    asset: dg.AssetsDefinition,
    entrypoint: str,
    subreddit: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The asset's real policy backs off 10s per retry; with three of them the failure
    # path alone would add minutes to an offline suite. Retry behaviour is asserted
    # separately in test_definitions.py, so this test only needs the terminal state.
    monkeypatch.setattr(asset.op, "_retry_policy", dg.RetryPolicy(max_retries=0))

    def failing_extract(*args: object) -> int:
        raise RuntimeError("reddit api down")

    monkeypatch.setattr(d, entrypoint, failing_extract)

    result = dg.materialize([asset], resources=resources(), raise_on_error=False)

    assert result.success is False
    assert result.get_asset_materialization_events() == [], "a failed extract has no row count to record"


@pytest.mark.parametrize(("asset", "table"), COPY_TABLES, ids=COPY_IDS)
def test_raw_asset_lands_its_own_table_via_copy_into(
    asset: dg.AssetsDefinition,
    table: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_copy_into(**kwargs: object) -> FakeCopyResult:
        calls.append(kwargs)
        return FakeCopyResult({table: ROWS_WRITTEN})

    monkeypatch.setattr(d, "copy_into_raw", fake_copy_into)
    # Every raw asset fetches before it COPY INTOs.
    monkeypatch.setattr(d, "extract_posts", lambda con: ROWS_WRITTEN)
    monkeypatch.setattr(d, "extract_comments", lambda con: ROWS_WRITTEN)
    monkeypatch.setattr(d, "extract_users", lambda con: ROWS_WRITTEN)

    result = dg.materialize([asset], resources=resources())

    assert result.success is True
    assert len(calls) == 1, "the asset lands its own table exactly once per materialization"
    assert calls[0]["tables"] == (table,)
    materializations = result.asset_materializations_for_node(asset.node_def.name)
    assert len(materializations) == 1
    assert materializations[0].metadata["rows_landed"].value == {table: ROWS_WRITTEN}
