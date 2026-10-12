"""Fixtures that wire the fakes into `ingestion.reddit._common`'s patch points."""

from __future__ import annotations

import logging
import sys
import types
from pathlib import Path

import dotenv
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # make `ingestion` importable when pytest is run from elsewhere
    sys.path.insert(0, str(REPO_ROOT))


def _no_dotenv(*_args: object, **_kwargs: object) -> bool:
    """Stand-in for `load_dotenv`, installed before the project modules import it."""
    return False


# `ingestion.reddit._common` and `flows.dbt_build_flow` call `load_dotenv()` at
# import time, so the stub has to be in place first: a developer's real .env must
# not reach os.environ during the test session.
dotenv.load_dotenv = _no_dotenv

from ingestion.reddit import _common  # noqa: E402
from tests.fakes import NOW, FakeConnection, FakeCursor, FakeReddit, FakeSubreddit  # noqa: E402

__all__ = ["NOW"]


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """The five required environment variables, with usable values."""
    for var in _common.REQUIRED_ENV:
        monkeypatch.setenv(var, f"test-{var.lower()}")
    return monkeypatch


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> float:
    """Pin `_common`'s clock to NOW without touching the global `time` module."""
    monkeypatch.setattr(_common, "time", types.SimpleNamespace(time=lambda: NOW))
    return NOW


@pytest.fixture
def make_warehouse(monkeypatch: pytest.MonkeyPatch):
    """Factory for a recording cursor wired into `_common.connect`."""

    def factory(described_columns: list[str] | None = None) -> FakeCursor:
        cursor = FakeCursor(described_columns)
        monkeypatch.setattr(_common.sql, "connect", lambda **_: FakeConnection(cursor))
        monkeypatch.setattr(
            _common,
            "Config",
            lambda **_: types.SimpleNamespace(authenticate=lambda: {"Authorization": "Bearer stub"}),
        )
        return cursor

    return factory


@pytest.fixture
def warehouse(make_warehouse) -> FakeCursor:
    return make_warehouse()


@pytest.fixture
def wire_reddit(monkeypatch: pytest.MonkeyPatch):
    """Factory installing a fake PRAW client that serves one subreddit."""

    def factory(subreddit: FakeSubreddit) -> None:
        monkeypatch.setattr(_common.praw, "Reddit", lambda **_: FakeReddit(subreddit))

    return factory


@pytest.fixture(autouse=True)
def _capture_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
