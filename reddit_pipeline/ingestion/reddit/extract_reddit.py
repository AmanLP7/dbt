"""Extract posts from every subreddit in the section seed and dump them into the run's SQLite database.

Importing this module only loads environment variables; all Reddit API and
SQLite work happens inside :func:`extract_posts`, so the module stays
importable and runnable (`python -m ingestion.reddit.extract_reddit`).
"""

import datetime
import sqlite3
from typing import Any

from ingestion.reddit._common import (
    INCREMENTAL_WINDOW_HOURS,
    POSTS_COLUMNS,
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

SQLITE_TABLE = "r_posts"


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


def _write_posts(
    con: sqlite3.Connection,
    submissions: list[Submission],
    cutoff: float,
    stamped_at: datetime.datetime,
    *,
    replace: bool = False,
) -> int:
    """Keep the submissions inside the read window and dump them to SQLite.

    Returns the number of rows written. Shared by the whole-seed load and the
    per-subreddit load so both apply the same window and the same dump.
    """
    fetched_count = len(submissions)
    inside_window = [submission for submission in submissions if submission.created_utc >= cutoff]
    logger.info(
        "Fetched %d posts; kept %d within the last %sh window.",
        fetched_count,
        len(inside_window),
        INCREMENTAL_WINDOW_HOURS,
    )
    rows = [row_values(POSTS_COLUMNS, _record(submission, stamped_at)) for submission in inside_window]
    return dump_rows(con, SQLITE_TABLE, POSTS_COLUMNS, rows, replace=replace)


def extract_posts_for(name: str, con: sqlite3.Connection | None = None) -> int:
    """Fetch one subreddit's listings and dump its posts to SQLite.

    One subreddit per call so each is a separate unit of work the orchestrator can
    materialise, retry and observe on its own: a subreddit that is private, banned
    or rate-limited fails this call without touching the others. Returns the
    number of rows written.

    `con` is the caller's already-open SQLite connection, with the table already
    created. The orchestrator opens one connection per run and shares it across
    the per-subreddit loads. Omitted, this opens and closes its own, which is what
    a hand run does.
    """
    validate_env()
    cutoff = window_cutoff()
    stamped_at = load_timestamp()

    logger.info("Fetching new and top-of-day posts from r/%s...", name)
    reddit = reddit_client()
    # A single name here, so `harvest_submissions` cannot swallow the failure: an
    # unreadable subreddit raises out of the listing call and fails this load.
    submissions, failed = harvest_submissions(reddit, [name])
    if failed:
        raise RuntimeError(f"Could not read r/{name}; see the log for the API error")

    if con is None:
        own_con = connect_sqlite()
        try:
            ensure_sqlite_table(own_con, SQLITE_TABLE, POSTS_COLUMNS)
            written = _write_posts(own_con, submissions, cutoff, stamped_at, replace=True)
        finally:
            own_con.close()
    else:
        written = _write_posts(con, submissions, cutoff, stamped_at)

    logger.info("r/%s: %d posts written.", name, written)
    return written


def extract_posts() -> int:
    """Fetch posts from every subreddit in the seed and dump them into SQLite.

    The whole-seed path, kept for `python -m ingestion.reddit.extract_reddit`: one
    API client and one SQLite connection for all the seeded subreddits, where the
    orchestrator instead calls :func:`extract_posts_for` once per subreddit.
    Returns the number of post rows written.
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

    # One load-time timestamp for every row of this run
    stamped_at = load_timestamp()

    logger.info("Opening SQLite run database...")
    con = connect_sqlite()
    try:
        ensure_sqlite_table(con, SQLITE_TABLE, POSTS_COLUMNS)
        logger.info("Dumping data into %s...", SQLITE_TABLE)
        written = _write_posts(con, submissions, cutoff, stamped_at, replace=True)
        logger.info("Data dump complete: %d posts written.", written)
    finally:
        con.close()

    return written


if __name__ == "__main__":
    configure_logging()
    extract_posts()
