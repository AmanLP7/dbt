"""Loading Bluesky Jetstream events from SQLite into Databricks.

Each source gets its own subpackage holding its loaders — currently just
`ingestion/bluesky/`, holding `load_events`. The loader runs standalone
(`python -m ingestion.bluesky.load_events`); this package sits outside dbt's
`models/` path on purpose, because dbt claims every `.py` file under `models/` as
a Python model.
"""

from __future__ import annotations
