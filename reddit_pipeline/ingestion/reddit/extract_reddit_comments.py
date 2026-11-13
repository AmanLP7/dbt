"""Fetch the top comments of the run's most-discussed posts and dump them into the run's SQLite database.

Importing this module only loads environment variables; all Reddit API and
SQLite work happens inside :func:`extract_comments`, so the module stays
importable and runnable (`python -m ingestion.reddit.extract_reddit_comments`).
"""

import datetime
import sqlite3
from typing import Any

from ingestion.reddit._common import (
    COMMENT_POST_LIMIT,
    COMMENTS_COLUMNS,
    COMMENTS_PER_POST,
    INCREMENTAL_WINDOW_HOURS,
    Submission,
    configure_logging,
    connect_sqlite,
    dump_rows,
    ensure_sqlite_table,
    harvest_submissions,
    load_timestamp,
    logger,
    reddit_client,
    row_values,
    subreddit_list,
    validate_env,
    window_cutoff,
)

SQLITE_TABLE = "r_comments"


def _comment_rows(submission: Submission, stamped_at: datetime.datetime) -> list[dict[str, Any]]:
    """The raw comment rows of one post, in `COMMENTS_COLUMNS`' own vocabulary.

    The subreddit is taken from the submission rather than from the comment: a
    comment carries no subreddit field of its own, and the post it hangs off is
    the only thing that knows which section the discussion belongs to.
    """
    # Resolve the comment forest without expanding "more" stubs
    submission.comments.replace_more(limit=0)
    return [
        {
            "id": comment.id,
            "post_id": submission.id,
            "body": comment.body if comment.body else "",
            "score": int(comment.score),
            "author": str(comment.author) if comment.author else "[deleted]",
            "created_utc": float(comment.created_utc),
            "parent_id": comment.parent_id,
            "subreddit": str(submission.subreddit).lower(),
            "ingested_at": stamped_at,
        }
        for comment in submission.comments[:COMMENTS_PER_POST]
    ]


def extract_comments(con: sqlite3.Connection | None = None) -> int:
    """Extract the top comments of the busiest posts in the window into SQLite.

    Returns the number of comment rows written: those fetched inside the rolling
    window. Rows outside the window are ignored.

    `con` is the caller's already-open SQLite connection, with the table already
    created. Omitted, this opens and closes its own, which is what a hand run
    does.
    """
    validate_env()

    # Rolling-window cutoff on the entity's own created_utc, computed once per run
    cutoff = window_cutoff()

    logger.info("Initializing Reddit API client...")
    reddit = reddit_client()

    names = subreddit_list()
    logger.info("Fetching new and top-of-day posts from %d subreddits...", len(names))
    submissions, failed = harvest_submissions(reddit, names)
    # Comment fetching is one request per post, so it is aimed at the posts whose
    # discussion is actually worth having, not at whatever order the listings
    # returned: by comment count, inside the window only.
    window = [submission for submission in submissions if submission.created_utc >= cutoff]
    window.sort(key=lambda submission: submission.num_comments, reverse=True)
    targets = window[:COMMENT_POST_LIMIT]
    logger.info(
        "Selected %d of %d in-window posts across %d subreddits (%d unreadable) for comment fetching.",
        len(targets),
        len(window),
        len(names) - len(failed),
        len(failed),
    )

    stamped_at = load_timestamp()
    comments: list[dict[str, Any]] = []
    for submission in targets:
        logger.debug("Fetching comments for post: %s...", submission.title[:50])
        comments.extend(_comment_rows(submission, stamped_at))
    logger.info("Successfully fetched %d comments.", len(comments))

    fetched_count = len(comments)
    comments = [comment for comment in comments if comment["created_utc"] >= cutoff]
    logger.info(
        "Fetched %d comments; kept %d within the last %sh window.",
        fetched_count,
        len(comments),
        INCREMENTAL_WINDOW_HOURS,
    )

    rows = [row_values(COMMENTS_COLUMNS, comment) for comment in comments]

    if con is None:
        own_con = connect_sqlite()
        try:
            ensure_sqlite_table(own_con, SQLITE_TABLE, COMMENTS_COLUMNS)
            written = dump_rows(own_con, SQLITE_TABLE, COMMENTS_COLUMNS, rows, replace=True)
        finally:
            own_con.close()
    else:
        written = dump_rows(con, SQLITE_TABLE, COMMENTS_COLUMNS, rows)

    logger.info("Comment data dump complete: %d comments written.", written)
    return written


if __name__ == "__main__":
    configure_logging()
    extract_comments()
