"""Extract Reddit data into the raw tables, then trigger the dbt build.

This flow owns the Reddit API calls and the raw Delta loads and nothing else;
the dbt models it needs afterwards are built by `flows.dbt_build_flow`, which it
triggers on success. Run it from the repo root:

    python -m flows.reddit_extract_flow
"""

from __future__ import annotations

from prefect import flow, task

# `ingestion.reddit._common` loads `.env` at import time, so importing the
# extractors below is what puts the DATABRICKS_* / REDDIT_* values in os.environ.
from flows.dbt_build_flow import dbt_build_flow
from ingestion.reddit.extract_reddit import extract_posts
from ingestion.reddit.extract_reddit_comments import extract_comments
from ingestion.reddit.extract_reddit_users import extract_users


@task(name="fetch-posts", retries=3, retry_delay_seconds=[10, 60, 300], retry_jitter_factor=0.5, log_prints=True)
def fetch_posts() -> int:
    """Fetch hot posts from r/worldnews and insert them into Databricks."""
    return extract_posts()


@task(name="fetch-comments", retries=3, retry_delay_seconds=[10, 60, 300], retry_jitter_factor=0.5, log_prints=True)
def fetch_comments() -> int:
    """Fetch comments for the hot posts and insert them into Databricks."""
    return extract_comments()


@task(name="fetch-users", retries=3, retry_delay_seconds=[10, 60, 300], retry_jitter_factor=0.5, log_prints=True)
def fetch_users() -> int:
    """Fetch the authors of the hot posts and insert them into Databricks."""
    return extract_users()


@flow(name="reddit-extract", log_prints=True)
def reddit_extract_flow() -> dict[str, int]:
    """Load the raw tables, then hand over to the dbt build.

    The three extractors run in order — comments and users derive from the same
    hot posts, so posts must land first. Only once they have all succeeded does
    the build flow run, so dbt never sees a half-loaded window.
    """
    posts = fetch_posts()
    comments = fetch_comments()
    users = fetch_users()

    # Triggered as a subflow rather than a separate deployment: `dbt-build` has
    # no schedule of its own and runs inline here, so a failed build fails this
    # run. `reddit-extract` itself is deployed to the `reddit-pool` work pool for
    # on-demand runs (see prefect.yaml); `dbt-build` is not deployed separately.
    dbt_build_flow()

    return {"posts": posts, "comments": comments, "users": users}


if __name__ == "__main__":
    reddit_extract_flow()
