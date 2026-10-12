"""The extract flow: load order, the dbt hand-off, and failure propagation."""

from __future__ import annotations

import pytest

from flows import dbt_build_flow, reddit_extract_flow


@pytest.fixture
def stubbed_extractors(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace the three load tasks with recorders; returns the call order."""
    order: list[str] = []
    for name, rows in (("fetch_posts", 11), ("fetch_comments", 22), ("fetch_users", 33)):
        monkeypatch.setattr(reddit_extract_flow, name, lambda n=name, r=rows: order.append(n) or r)
    return order


@pytest.fixture
def triggered(monkeypatch: pytest.MonkeyPatch, stubbed_extractors: list[str]) -> list[str]:
    """Record the dbt hand-off in the same call order, without running it."""
    monkeypatch.setattr(dbt_build_flow, "run_dbt_build", lambda: stubbed_extractors.append("dbt"))
    monkeypatch.setattr(reddit_extract_flow, "dbt_build_flow", lambda *a, **kw: stubbed_extractors.append("dbt"))
    return stubbed_extractors


def test_extract_flow_loads_posts_then_comments_then_users(stubbed_extractors, triggered):
    # Comments and users derive from the same hot posts, so the order matters, and
    # the dbt step must not start before the three loads have finished.
    result = reddit_extract_flow.reddit_extract_flow.fn()

    assert stubbed_extractors == ["fetch_posts", "fetch_comments", "fetch_users", "dbt"]
    assert result == {"posts": 11, "comments": 22, "users": 33}


def test_a_failed_extractor_does_not_trigger_the_dbt_build(triggered, monkeypatch):
    def boom() -> int:
        raise RuntimeError("reddit api down")

    monkeypatch.setattr(reddit_extract_flow, "fetch_comments", boom)

    with pytest.raises(RuntimeError, match="reddit api down"):
        reddit_extract_flow.reddit_extract_flow.fn()

    assert triggered == ["fetch_posts"]


def test_a_failed_dbt_build_fails_the_extract_flow(monkeypatch: pytest.MonkeyPatch):
    """The real hand-off: no deployment or server needed, and it cannot be swallowed."""
    monkeypatch.setattr(reddit_extract_flow, "fetch_posts", lambda: 1)
    monkeypatch.setattr(reddit_extract_flow, "fetch_comments", lambda: 2)
    monkeypatch.setattr(reddit_extract_flow, "fetch_users", lambda: 3)

    def boom() -> None:
        raise RuntimeError("dbt build failed")

    monkeypatch.setattr(dbt_build_flow, "run_dbt_build", boom)

    with pytest.raises(RuntimeError, match="dbt build failed"):
        reddit_extract_flow.reddit_extract_flow()
