"""Reddit extraction and loading into Databricks.

Each source gets its own subpackage holding its extractors — currently just
`ingestion/reddit/`. The extractors run standalone
(`python -m ingestion.reddit.extract_reddit`) or together through the Dagster code
location in `orchestration/`. This package sits outside dbt's `models/` path on
purpose: dbt claims every `.py` file under `models/` as a Python model, which these
are not.
"""
