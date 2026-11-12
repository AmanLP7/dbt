"""Behaviour of the extractors, exercised against stubbed Reddit and Databricks clients."""

from __future__ import annotations

import logging

import pytest

from ingestion.reddit import _common
from ingestion.reddit.extract_reddit import extract_posts
from ingestion.reddit.extract_reddit_comments import extract_comments
from ingestion.reddit.extract_reddit_users import extract_users
from tests.fakes import NOW, WINDOW, BrokenRedditor, FakeComment, FakeRedditor, FakeSubmission, FakeSubreddit

# Every test needs the credentials set and the clock pinned, but reads neither.
pytestmark = pytest.mark.usefixtures("env", "clock")

HOUR = 3600
INSIDE = NOW - HOUR
OUTSIDE = NOW - (WINDOW + 1) * HOUR
POSTS_TABLE = _common.full_table("r_posts")
COMMENTS_TABLE = _common.full_table("r_comments")
USERS_TABLE = _common.full_table("r_users")


def test_posts_loads_only_rows_inside_the_window(warehouse, wire_reddit):
    wire_reddit(
        FakeSubreddit([FakeSubmission("fresh", created_utc=INSIDE), FakeSubmission("stale", created_utc=OUTSIDE)])
    )

    written = extract_posts()

    assert written == 1
    rows = warehouse.inserted(_common.POSTS_COLUMNS)
    assert [row[0] for row in rows] == ["fresh"]
    # The load is an upsert on the fetched ids, not a window rewrite: two runs
    # cover different spans of the same day, so a time-based delete would drop
    # rows the run never refetched.
    delete_sql, params = warehouse.deletes[0]
    assert delete_sql.startswith(f"DELETE FROM {POSTS_TABLE}")
    assert params == ("fresh",)


def test_a_post_seen_in_both_listings_is_written_once(warehouse, wire_reddit):
    post = FakeSubmission("p1", created_utc=INSIDE)
    fake = wire_reddit(FakeSubreddit([post], top=[post]))

    written = extract_posts()

    assert written == 1
    assert [row[0] for row in warehouse.inserted(_common.POSTS_COLUMNS)] == ["p1"]
    # `top` without a day filter is an all-time top 100: the 26-hour read filter
    # would then discard nearly all of it, silently, and the "already earned its
    # place" half of the harvest would stop contributing.
    assert fake.served("worldnews").top_time_filter == _common.TOP_TIME_FILTER


def test_each_row_carries_its_own_subreddit(warehouse, wire_reddit):
    # The edition's section map joins on this column, so a row that inherited the
    # first subreddit fetched would print under the wrong section.
    wire_reddit(
        FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, subreddit="technology")], name="technology"),
        name="technology",
    )

    extract_posts()

    rows = warehouse.inserted(_common.POSTS_COLUMNS)
    subreddit_index = list(_common.POSTS_COLUMNS).index("subreddit")
    assert {row[subreddit_index] for row in rows} == {"technology"}


def test_an_unreadable_subreddit_is_reported_and_skipped(warehouse, wire_reddit, caplog):
    # Two of the seed's subreddits fail to read; the third still loads.
    wire_reddit(
        FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, subreddit="technology")], name="technology"),
        name="technology",
        unreadable=("worldnews", "politics"),
    )

    with caplog.at_level(logging.WARNING):
        written = extract_posts()

    assert written == 1  # the readable subreddit still loaded
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("r/worldnews" in message for message in warnings)
    assert any("r/politics" in message for message in warnings)


def test_the_seed_is_the_only_list_of_subreddits():
    # Adding a subreddit must stay a one-file data change, and the ingest must not
    # drift from the seed the marts join on.
    names = _common.subreddit_list()

    assert len(names) == len(set(names)), "a duplicated seed row would double every request"
    assert "worldnews" in names and "askreddit" in names
    assert all(name == name.lower() for name in names)


def test_posts_writes_the_ingest_timestamp_on_every_row(warehouse, wire_reddit):
    wire_reddit(FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE)]))

    extract_posts()

    rows = warehouse.inserted(_common.POSTS_COLUMNS)
    assert len(rows[0]) == len(_common.POSTS_COLUMNS)
    assert rows[0][-1] is not None  # ingested_at, stamped at load time


def test_empty_window_skips_both_delete_and_insert(warehouse, wire_reddit):
    wire_reddit(FakeSubreddit([FakeSubmission("stale", created_utc=OUTSIDE)]))

    written = extract_posts()

    assert written == 0
    assert warehouse.deletes == []
    assert warehouse.inserts == []


def test_comment_values_are_bound_never_interpolated(warehouse, wire_reddit):
    body = "O'Brien; DROP TABLE workspace.topic_raw.r_comments; --"
    comment = FakeComment("c1", "p1", body, 5, FakeRedditor("alice"), INSIDE)
    wire_reddit(FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, comments=[comment])]))

    written = extract_comments()

    assert written == 1
    insert_sql, _ = warehouse.inserts[0]
    assert "?" in insert_sql
    assert body not in warehouse.sql_text  # the payload never reaches the SQL string
    assert body in [row[2] for row in warehouse.inserted(_common.COMMENTS_COLUMNS)]  # it travels bound


def test_a_table_predating_the_ingest_column_is_migrated(make_warehouse, wire_reddit):
    # A table created before `ingested_at` existed: DESCRIBE reports the old shape.
    legacy_columns = [name for name in _common.COMMENTS_COLUMNS if name != "ingested_at"]
    cursor = make_warehouse(described_columns=legacy_columns)
    wire_reddit(FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, comments=[])]))

    extract_comments()

    assert cursor.alters == [f"ALTER TABLE {COMMENTS_TABLE} ADD COLUMNS (ingested_at TIMESTAMP);"]
    assert cursor.inserts == []


def test_a_current_table_is_not_altered(warehouse, wire_reddit):
    wire_reddit(FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, comments=[])]))

    extract_comments()

    assert warehouse.alters == []


def test_users_skips_deleted_authors_and_duplicates(warehouse, wire_reddit):
    posts = [
        FakeSubmission("p1", author=FakeRedditor("alice"), created_utc=INSIDE),
        FakeSubmission("p2", author=FakeRedditor("alice"), created_utc=INSIDE),  # duplicate name
        FakeSubmission("p3", author=None, created_utc=INSIDE),  # deleted author
    ]
    wire_reddit(FakeSubreddit(posts))

    written = extract_users()

    assert written == 1
    rows = warehouse.inserted(_common.USERS_COLUMNS)
    assert [row[0] for row in rows] == ["alice"]
    # Key-based upsert: the fetched id is bound into the DELETE, not string-escaped.
    delete_sql, params = warehouse.deletes[0]
    assert delete_sql == f"DELETE FROM {USERS_TABLE} WHERE id IN (?);"
    assert params == ("t2_alice",)


def test_users_reports_unreadable_profiles_instead_of_swallowing_them(warehouse, wire_reddit, caplog):
    wire_reddit(FakeSubreddit([FakeSubmission("p1", author=BrokenRedditor(), created_utc=INSIDE)]))

    with caplog.at_level(logging.WARNING):
        written = extract_users()

    assert written == 0
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("ghost" in message for message in warnings)
