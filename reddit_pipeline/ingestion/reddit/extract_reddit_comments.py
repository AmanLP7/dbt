"""Fetch hot r/worldnews posts with their top comments and load them into Databricks.

Importing this module only loads environment variables; all Reddit API and
Databricks work happens inside :func:`extract_comments`, so the module stays
importable and runnable (`python -m ingestion.reddit.extract_reddit_comments`).
"""

from ingestion.reddit._common import (
    COMMENTS_COLUMNS,
    COMMENTS_PER_POST,
    INCREMENTAL_WINDOW_HOURS,
    POSTS_LIMIT,
    SUBREDDIT,
    configure_logging,
    connect,
    ensure_columns,
    ensure_table,
    full_table,
    load_timestamp,
    logger,
    reddit_client,
    replace_window,
    validate_env,
    window_cutoff,
)

TABLE = full_table("r_comments")


def extract_comments() -> int:
    """Extract the top comments of the hot r/worldnews posts into Databricks.

    Returns the number of comment rows written: those fetched inside the rolling
    window. Rows outside the window are left untouched.
    """
    validate_env()

    # Rolling-window cutoff on the entity's own created_utc, computed once per run
    cutoff = window_cutoff()

    logger.info("Initializing Reddit API client...")
    reddit = reddit_client()

    logger.info("Fetching hot posts from r/%s and their comments...", SUBREDDIT)
    subreddit = reddit.subreddit(SUBREDDIT)
    comments = []
    for submission in subreddit.hot(limit=POSTS_LIMIT):
        logger.debug("Fetching comments for post: %s...", submission.title[:50])
        # Resolve the comment forest without expanding "more" stubs
        submission.comments.replace_more(limit=0)
        for comment in submission.comments[:COMMENTS_PER_POST]:
            comments.append(
                {
                    "id": comment.id,
                    "post_id": submission.id,
                    "body": comment.body if comment.body else "",
                    "score": int(comment.score),
                    "author": str(comment.author) if comment.author else "[deleted]",
                    "created_utc": float(comment.created_utc),
                    "parent_id": comment.parent_id,
                }
            )
    logger.info("Successfully fetched %d comments.", len(comments))

    fetched_count = len(comments)
    comments = [comment for comment in comments if comment["created_utc"] >= cutoff]
    logger.info(
        "Fetched %d comments; kept %d within the last %sh window.",
        fetched_count,
        len(comments),
        INCREMENTAL_WINDOW_HOURS,
    )

    # One load-time timestamp for every row of this run
    stamped_at = load_timestamp()
    rows = [
        (
            comment["id"],
            comment["post_id"],
            comment["body"],
            comment["score"],
            comment["author"],
            comment["created_utc"],
            comment["parent_id"],
            stamped_at,
        )
        for comment in comments
    ]

    logger.info("Connecting to Databricks...")
    connection, cursor = connect()
    try:
        ensure_table(cursor, TABLE, COMMENTS_COLUMNS)
        ensure_columns(cursor, TABLE, COMMENTS_COLUMNS)
        logger.info("Ingesting data into %s...", TABLE)
        written = replace_window(cursor, TABLE, COMMENTS_COLUMNS, rows, "created_utc", cutoff)
        logger.info("Comment data ingestion complete: %d comments written.", written)
    finally:
        cursor.close()
        connection.close()

    return written


if __name__ == "__main__":
    configure_logging()
    extract_comments()
