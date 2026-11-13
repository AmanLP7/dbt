"""Reddit extractors: the source-specific half of the ingestion package.

`extract_reddit.py`, `extract_reddit_comments.py` and `extract_reddit_users.py`
each expose a single entrypoint (`extract_posts()`, `extract_comments()`,
`extract_users()`) that fetches from Reddit and dumps its rows into a local SQLite
run database (`data/reddit.db` by default). `copy_into.py` then exports that dump
as NDJSON, uploads it to a Unity Catalog volume, and runs `COPY INTO` to land it in
the three Databricks raw tables. Importing a module runs no extraction; each is also
runnable directly (`python -m ingestion.reddit.extract_reddit` from the repo root),
where `__main__` configures logging and calls the entrypoint.
"""
