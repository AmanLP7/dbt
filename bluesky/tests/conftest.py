"""Fixtures and import wiring for the offline loader tests: no SQLite file to seed,
no warehouse to reach."""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import types
from pathlib import Path

import dotenv
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # make `ingestion` importable when pytest is run from elsewhere
    sys.path.insert(0, str(REPO_ROOT))

# The loader reads connection settings from the environment, and its module-level
# `load_dotenv()` runs at import. Neither may depend on the developer's own .env
# or credentials, so the stub is installed before the first project import.
dotenv.load_dotenv = lambda *_args, **_kwargs: False  # type: ignore[assignment]

os.environ.setdefault("DATABRICKS_HOSTNAME", "https://example.invalid")
os.environ.setdefault("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/000")
os.environ.setdefault("DATABRICKS_CONFIG_FILE", "/nonexistent/databrickscfg")

from ingestion.bluesky import _common  # noqa: E402
from tests.fakes import (  # noqa: E402
    INGESTED_AT,
    FakeConnection,
    FakeWarehouse,
    event_row,
)

__all__ = ["INGESTED_AT", "FakeConnection", "FakeWarehouse", "event_row"]


@pytest.fixture(autouse=True)
def _capture_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)


@pytest.fixture
def source_db(tmp_path: Path):
    """Factory for a SQLite file with the schema `main.events` declares."""

    def build(rows: list[tuple[object, ...]]) -> Path:
        path = tmp_path / "bluesky.db"
        con = sqlite3.connect(path)
        con.execute(
            "CREATE TABLE events (record_key TEXT NOT NULL, ingested_at TEXT NOT NULL, event_dt TEXT, "
            "event_kind TEXT, collection TEXT, did TEXT, operation TEXT, rkey TEXT, "
            "seq INTEGER PRIMARY KEY, topic TEXT, partition INTEGER, offset INTEGER, payload TEXT NOT NULL)"
        )
        con.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.commit()
        con.close()
        return path

    return build


@pytest.fixture
def warehouse(monkeypatch: pytest.MonkeyPatch) -> FakeWarehouse:
    """A fake warehouse wired into `_common.connect`."""
    fake = FakeWarehouse()
    monkeypatch.setattr(_common, "connect", fake.connect)
    return fake


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> types.SimpleNamespace:
    """A controllable `perf_counter`, injected into the loader module only."""

    class ManualClock:
        def __init__(self) -> None:
            self.now = 0.0

        def __call__(self) -> float:
            return self.now

    manual = ManualClock()
    from ingestion.bluesky import load_events

    monkeypatch.setattr(load_events, "time", types.SimpleNamespace(perf_counter=manual))
    return types.SimpleNamespace(advance=lambda seconds: setattr(manual, "now", manual.now + seconds), now=manual)
