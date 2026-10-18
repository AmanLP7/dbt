"""Make `ingestion` importable and keep the developer's `.env` out of the tests.

This module is imported before any project module, which matters because
`ingestion.jobs._common` calls `load_dotenv()` at import time (mirroring the
sibling project): a real `.env` pointing at a live workspace must not reach
`os.environ` during a test session, so `dotenv` is stubbed beforehand.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # make `ingestion` importable when pytest is run from elsewhere
    sys.path.insert(0, str(REPO_ROOT))

_stub: Any = types.ModuleType("dotenv")
_stub.load_dotenv = lambda *_args, **_kwargs: False
sys.modules.setdefault("dotenv", _stub)


@pytest.fixture
def databricks_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """The required connection settings, pointing at a host that is never dialled.

    No `DATABRICKS_TOKEN`: the extractor authenticates from the CLI profile in
    `~/.databrickscfg`, which refreshes its own token.
    """
    env = {
        "DATABRICKS_HOSTNAME": "https://example.invalid",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/000",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    return env
