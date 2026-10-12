"""Collect r/worldnews users from Reddit and load them into Databricks.

Importing this module only loads environment variables; all Reddit API and
Databricks work happens inside :func:`extract_users`, so the module stays
importable and runnable (`python -m ingestion.reddit.extract_reddit_users`).
"""

from ingestion.reddit._common import (
    POSTS_LIMIT,
    SUBREDDIT,
    TARGET_USER_COUNT,
    USER_COMMENTS_PER_POST,
    USERS_COLUMNS,
    configure_logging,
    connect,
    ensure_columns,
    ensure_table,
    full_table,
    load_timestamp,
    logger,
    reddit_client,
    replace_by_key,
    validate_env,
)

TABLE = full_table("r_users")


def extract_users() -> int:
    """Extract r/worldnews user profiles and upsert them into Databricks.

    Returns the number of user rows written. The load is key-based on `id`, so a
    user seen again this run replaces its previous row rather than duplicating it.
    """
    validate_env()

    logger.info("Initializing Reddit API client...")
    reddit = reddit_client()

    logger.info(
        "Collecting %d unique users who post/comment on r/%s...",
        TARGET_USER_COUNT,
        SUBREDDIT,
    )
    users: list[dict] = []
    seen_usernames: set[str] = set()
    failed_profiles = 0

    def collect(author, source: str) -> bool:
        """Append one user profile the first time its author is seen.

        Reading a profile's fields is what triggers the per-user API request, so a
        suspended, deleted or private account raises here; that failure is logged
        and counted rather than swallowed.
        """
        nonlocal failed_profiles
        if not author or author.name in seen_usernames or author.name == "[deleted]":
            return False
        seen_usernames.add(author.name)
        try:
            users.append(
                {
                    "username": author.name,
                    "id": author.id,
                    "created_utc": float(author.created_utc),
                    "link_karma": int(author.link_karma),
                    "comment_karma": int(author.comment_karma),
                    "is_employee": bool(author.is_employee),
                    "is_gold": bool(author.is_gold),
                }
            )
        except Exception as exc:  # suspended / deleted / private user
            failed_profiles += 1
            logger.warning("Failed to read profile for u/%s (%s author): %s", author.name, source, exc)
            return False
        logger.debug("[%d/%d] Collected user from %s: %s", len(users), TARGET_USER_COUNT, source, author.name)
        return True

    subreddit = reddit.subreddit(SUBREDDIT)

    # 1. Hot submissions: the post author, then the authors of its top comments
    logger.info("Fetching authors from hot submissions...")
    for submission in subreddit.hot(limit=POSTS_LIMIT):
        if len(users) >= TARGET_USER_COUNT:
            break
        collect(submission.author, "post")

        if len(users) >= TARGET_USER_COUNT:
            break
        try:
            submission.comments.replace_more(limit=0)
        except Exception as exc:
            logger.warning("Error reading comments for post %s: %s", submission.id, exc)
            continue
        for comment in submission.comments[:USER_COMMENTS_PER_POST]:
            if len(users) >= TARGET_USER_COUNT:
                break
            collect(comment.author, "comment")

    # 2. Fallback: new submissions, if the hot posts and their comments fell short
    if len(users) < TARGET_USER_COUNT:
        logger.info(
            "Collected %d users. Fetching from new submissions to reach %d...",
            len(users),
            TARGET_USER_COUNT,
        )
        for submission in subreddit.new(limit=POSTS_LIMIT):
            if len(users) >= TARGET_USER_COUNT:
                break
            collect(submission.author, "new post")

    logger.info(
        "Successfully retrieved %d user profiles; %d profile fetches failed.",
        len(users),
        failed_profiles,
    )

    # One load-time timestamp for every row of this run
    stamped_at = load_timestamp()
    rows = [
        (
            user["username"],
            user["id"],
            user["created_utc"],
            user["link_karma"],
            user["comment_karma"],
            user["is_employee"],
            user["is_gold"],
            stamped_at,
        )
        for user in users
    ]

    logger.info("Connecting to Databricks...")
    connection, cursor = connect()
    try:
        ensure_table(cursor, TABLE, USERS_COLUMNS)
        ensure_columns(cursor, TABLE, USERS_COLUMNS)
        logger.info("Ingesting user data into %s...", TABLE)
        written = replace_by_key(cursor, TABLE, USERS_COLUMNS, rows, "id")
        logger.info("User data ingestion complete: %d users written.", written)
    finally:
        cursor.close()
        connection.close()

    return written


if __name__ == "__main__":
    configure_logging()
    extract_users()
