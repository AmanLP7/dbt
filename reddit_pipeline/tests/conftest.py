"""Fixtures that wire the fakes into `ingestion.reddit._common`'s patch points."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import types
from pathlib import Path

import dotenv
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # make `ingestion` importable when pytest is run from elsewhere
    sys.path.insert(0, str(REPO_ROOT))

# The code location builds its dbt assets from `target/manifest.json` at import time,
# and `prepare_if_dev()` there only refreshes it under `dagster dev`. Parsing here,
# before any test module imports the location, makes collection independent of what
# `target/` happens to hold: a fresh checkout has no manifest at all, and a partial
# parse cache left by an interrupted run can produce one whose dependency edges no
# longer match the models. `--no-partial-parse` costs about a second over a cached
# parse and removes that second source of drift.
#
# The committed profile resolves DATABRICKS_* through `env_var(...)`, and a parse
# needs the values present but never opens a connection.
os.environ.setdefault("DATABRICKS_HOSTNAME", "https://example.invalid")
os.environ.setdefault("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/000")
os.environ.setdefault("DATABRICKS_TOKEN", "dapi-test-placeholder")

# `definitions.py` mints a token from the Databricks CLI profile at import. Left to
# the developer's own `~/.databrickscfg` that would read ambient credentials, replace
# the placeholder above, and, when the cached token has expired, try to refresh it
# over the network. A config file that does not exist makes the mint fail fast with
# the documented fall-through instead.
os.environ.setdefault("DATABRICKS_CONFIG_FILE", "/nonexistent/databrickscfg")

subprocess.run(
    ["dbt", "parse", "--no-partial-parse", "--quiet", "--profiles-dir", "profiles"],
    cwd=REPO_ROOT,
    check=True,
)


def _no_dotenv(*_args: object, **_kwargs: object) -> bool:
    """Stand-in for `load_dotenv`, installed before the project modules import it."""
    return False


# `ingestion.reddit._common` calls `load_dotenv()` at import time, so the stub has
# to be in place first: a developer's real .env must not reach os.environ during the
# test session.
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
