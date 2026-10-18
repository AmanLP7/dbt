"""Dagster definitions for the Reddit pipeline.

The code location is `reddit_pipeline_orchestration.definitions`, which exposes `defs`.
"""

from __future__ import annotations

import sys
from pathlib import Path

# The dbt project *is* this repository, and `ingestion/` is a plain package at its root
# rather than an installed distribution: `python -m ingestion.reddit.extract_reddit`
# only resolves from the repo root. The orchestration package is installed (editable)
# and can be imported from anywhere, so it puts the repo root on `sys.path` itself --
# the same fix `tests/conftest.py` applies so pytest can import the extractors.
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
