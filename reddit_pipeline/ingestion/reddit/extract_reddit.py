"""Extract hot posts from r/worldnews and load them into Databricks.

Importing this module only loads environment variables; all Reddit API and
Databricks work happens inside :func:`extract_posts`, so the module stays
importable and runnable (`python -m ingestion.reddit.extract_reddit`).
"""

from ingestion.reddit._common import (
    INCREMENTAL_WINDOW_HOURS,
    POSTS_COLUMNS,
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

TABLE = full_table("r_worldnews")


def extract_posts() -> int:
    """Fetch the hot r/worldnews posts and load them into Databricks.

    Returns the number of post rows written: those fetched inside the rolling
    window. Rows outside the window are left untouched.
    """
    validate_env()

    # Compute the rolling-window cutoff once for this run
    cutoff = window_cutoff()

    logger.info("Initializing Reddit API client...")
    reddit = reddit_client()

    logger.info("Fetching hot posts from r/%s...", SUBREDDIT)
    subreddit = reddit.subreddit(SUBREDDIT)
    posts = [
        {
            "id": submission.id,
            "title": submission.title,
            "score": int(submission.score),
            "url": submission.url,
            "comms_num": int(submission.num_comments),
            "created_utc": float(submission.created_utc),
            "body": submission.selftext if submission.selftext else "",
            "author": str(submission.author) if submission.author else "[deleted]",
            "subreddit": SUBREDDIT,
        }
        for submission in subreddit.hot(limit=POSTS_LIMIT)
    ]
    logger.info("Successfully fetched %d posts.", len(posts))

    # Keep only posts inside the rolling window
    fetched_count = len(posts)
    posts = [post for post in posts if post["created_utc"] >= cutoff]
    logger.info(
        "Fetched %d posts; kept %d within the last %sh window.",
        fetched_count,
        len(posts),
        INCREMENTAL_WINDOW_HOURS,
    )

    # One load-time timestamp for every row of this run
    stamped_at = load_timestamp()
    rows = [
        (
            post["id"],
            post["title"],
            post["score"],
            post["url"],
            post["comms_num"],
            post["created_utc"],
            post["body"],
            post["author"],
            post["subreddit"],
            stamped_at,
        )
        for post in posts
    ]

    logger.info("Connecting to Databricks...")
    connection, cursor = connect()
    try:
        ensure_table(cursor, TABLE, POSTS_COLUMNS)
        ensure_columns(cursor, TABLE, POSTS_COLUMNS)
        logger.info("Ingesting data into %s...", TABLE)
        written = replace_window(cursor, TABLE, POSTS_COLUMNS, rows, "created_utc", cutoff)
        logger.info("Data ingestion complete: %d posts written.", written)
    finally:
        cursor.close()
        connection.close()

    return written


if __name__ == "__main__":
    configure_logging()
    extract_posts()
