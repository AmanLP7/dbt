"""Test-harness path setup for the Kafka flows under one `pytest` run.

Every flow is a self-contained tree whose modules are imported as
`ingestion.<flow>.*` from its own directory, so running a flow's suite on its own
needs nothing but that directory on `sys.path`. Collecting both in one run does
need something: each flow has a top-level `ingestion` package, and a plain
`sys.path` entry would let the first one shadow the second. Extending the
package's `__path__` with both roots is the standard way to merge them.

Nothing here is imported by the flows themselves; the entry points run as
`python -m ingestion.<flow>.<module>` from inside their flow directory.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).parent
FLOWS = ("bluesky", "wikimedia")


def _merge_ingestion_package() -> None:
    for flow in reversed(FLOWS):
        path = str(ROOT / flow)
        if path not in sys.path:
            sys.path.insert(0, path)

    ingestion = importlib.import_module("ingestion")
    for flow in FLOWS:
        candidate = str(ROOT / flow / "ingestion")
        if candidate not in ingestion.__path__:
            ingestion.__path__.append(candidate)


_merge_ingestion_package()
