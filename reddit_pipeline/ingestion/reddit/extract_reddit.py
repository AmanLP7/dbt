"""Extract posts from every subreddit in the section seed and load them into Databricks.

Importing this module only loads environment variables; all Reddit API and
Databricks work happens inside :func:`extract_posts`, so the module stays
importable and runnable (`python -m ingestion.reddit.extract_reddit`).
"""

import datetime
from typing import Any

from ingestion.reddit._common import (
    INCREMENTAL_WINDOW_HOURS,
    POSTS_COLUMNS,
    Submission,
    configure_logging,
    connect,
    ensure_columns,
    ensure_table,
    full_table,
    harvest_submissions,
    load_timestamp,
    logger,
    reddit_client,
    replace_by_key,
    row_values,
    subreddit_list,
    validate_env,
    window_cutoff,
)

TABLE = full_table("r_posts")


def _record(submission: Submission, stamped_at: datetime.datetime) -> dict[str, Any]:
    """One raw post row, in `POSTS_COLUMNS`' own vocabulary."""
    return {
        "id": submission.id,
        "title": submission.title,
        "score": int(submission.score),
        "url": submission.url,
        "comms_num": int(submission.num_comments),
        "created_utc": float(submission.created_utc),
        "body": submission.selftext if submission.selftext else "",
        "author": str(submission.author) if submission.author else "[deleted]",
        "subreddit": str(submission.subreddit).lower(),
        "ingested_at": stamped_at,
    }


def extract_posts() -> int:
    """Fetch posts from every subreddit in the seed and load them into Databricks.

    Returns the number of post rows written: those fetched inside the rolling
    window. Rows outside the window are ignored, and rows the table already holds
    for a fetched id are replaced.
    """
    validate_env()

    # Compute the rolling-window cutoff once for this run
    cutoff = window_cutoff()

    logger.info("Initializing Reddit API client...")
    reddit = reddit_client()

    names = subreddit_list()
    logger.info("Fetching new and top-of-day posts from %d subreddits...", len(names))
    submissions, failed = harvest_submissions(reddit, names)
    logger.info(
        "Fetched %d distinct posts from %d subreddits; %d subreddits unreadable.",
        len(submissions),
        len(names) - len(failed),
        len(failed),
    )

    # Keep only posts inside the read window
    fetched_count = len(submissions)
    submissions = [submission for submission in submissions if submission.created_utc >= cutoff]
    logger.info(
        "Fetched %d posts; kept %d within the last %sh window.",
        fetched_count,
        len(submissions),
        INCREMENTAL_WINDOW_HOURS,
    )

    # One load-time timestamp for every row of this run
    stamped_at = load_timestamp()
    rows = [row_values(POSTS_COLUMNS, _record(submission, stamped_at)) for submission in submissions]

    logger.info("Connecting to Databricks...")
    connection, cursor = connect()
    try:
        ensure_table(cursor, TABLE, POSTS_COLUMNS)
        ensure_columns(cursor, TABLE, POSTS_COLUMNS)
        logger.info("Ingesting data into %s...", TABLE)
        written = replace_by_key(cursor, TABLE, POSTS_COLUMNS, rows, "id")
        logger.info("Data ingestion complete: %d posts written.", written)
    finally:
        cursor.close()
        connection.close()

    return written


if __name__ == "__main__":
    configure_logging()
    extract_posts()
