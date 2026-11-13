"""Fixtures that wire the fakes into `ingestion.reddit._common`'s patch points."""

from __future__ import annotations

import logging
import os
import sqlite3
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
def sqlite_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    """A real SQLite dump at a temp path, with the three raw tables ready.

    The dump is a real file, not an in-memory `:memory:` connection. The
    whole-seed extractors open their *own* connection through
    `_common.connect_sqlite()`, which resolves its path from `sqlite_path()` at
    call time, so setting `REDDIT_SQLITE_PATH` to the temp file is what makes
    their writes land where this connection can read them — no need to patch
    `connect_sqlite` (which the extractors import by name anyway).
    """
    path = tmp_path / "reddit.db"
    monkeypatch.setenv(_common.REDDIT_SQLITE_PATH_ENV, str(path))
    con = _common.connect_sqlite()
    for table, columns in _common.RAW_TABLE_COLUMNS.items():
        _common.ensure_sqlite_table(con, table, columns)
    return con


@pytest.fixture
def copy_warehouse(monkeypatch: pytest.MonkeyPatch) -> FakeCursor:
    """A recording Databricks cursor wired into `_common.connect`, for the COPY loader.

    The COPY loader is the only part of the pipeline that still talks to Databricks:
    its `connect()` is patched to return a fake connection/cursor pair, and `Config`
    to a stub that never shells out to the CLI.
    """
    cursor = FakeCursor()
    monkeypatch.setattr(_common.sql, "connect", lambda **_: FakeConnection(cursor))
    monkeypatch.setattr(
        _common,
        "Config",
        lambda **_: types.SimpleNamespace(authenticate=lambda: {"Authorization": "Bearer stub"}),
    )
    return cursor


@pytest.fixture
def wire_reddit(monkeypatch: pytest.MonkeyPatch):
    """Factory installing a fake PRAW client that serves one subreddit."""

    def factory(subreddit: FakeSubreddit, *, name: str = "worldnews", unreadable: tuple[str, ...] = ()) -> FakeReddit:
        fake = FakeReddit(subreddit, name=name, unreadable=unreadable)
        monkeypatch.setattr(_common.praw, "Reddit", lambda **_: fake)
        return fake

    return factory


@pytest.fixture(autouse=True)
def _capture_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
