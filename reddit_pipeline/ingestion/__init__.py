"""Reddit extraction and loading into Databricks.

Each source gets its own subpackage holding its extractors — currently just
`ingestion/reddit/`. The extractors run standalone
(`python ingestion/reddit/extract_reddit.py`) or together through the Prefect flow
in `flows/` (`python -m flows.reddit_extract_flow`). This package sits outside
dbt's `models/` path on purpose: dbt claims every `.py` file under `models/` as a
Python model, which these are not.
"""
