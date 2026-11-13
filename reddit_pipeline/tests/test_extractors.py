"""Behaviour of the extractors, exercised against stubbed Reddit and a real SQLite dump.

The extractors no longer touch Databricks: each writes the rows it fetched into a
local SQLite database, which `copy_into_raw` later exports to the warehouse. These
tests assert against the SQLite contents, so a row that landed (or was dropped by
the window filter) is visible in `SELECT` results rather than in a recorded INSERT.
"""

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


def test_posts_loads_only_rows_inside_the_window(sqlite_db, wire_reddit):
    wire_reddit(
        FakeSubreddit([FakeSubmission("fresh", created_utc=INSIDE), FakeSubmission("stale", created_utc=OUTSIDE)])
    )

    written = extract_posts()

    assert written == 1
    rows = sqlite_db.execute("SELECT id FROM r_posts").fetchall()
    assert [row[0] for row in rows] == ["fresh"]


def test_a_post_seen_in_both_listings_is_written_once(sqlite_db, wire_reddit):
    post = FakeSubmission("p1", created_utc=INSIDE)
    fake = wire_reddit(FakeSubreddit([post], top=[post]))

    written = extract_posts()

    assert written == 1
    assert [row[0] for row in sqlite_db.execute("SELECT id FROM r_posts")] == ["p1"]
    # `top` without a day filter is an all-time top 100: the 26-hour read filter
    # would then discard nearly all of it, silently, and the "already earned its
    # place" half of the harvest would stop contributing.
    assert fake.served("worldnews").top_time_filter == _common.TOP_TIME_FILTER


def test_each_row_carries_its_own_subreddit(sqlite_db, wire_reddit):
    # The edition's section map joins on this column, so a row that inherited the
    # first subreddit fetched would print under the wrong section.
    wire_reddit(
        FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, subreddit="technology")], name="technology"),
        name="technology",
    )

    extract_posts()

    rows = sqlite_db.execute("SELECT subreddit FROM r_posts").fetchall()
    assert {row[0] for row in rows} == {"technology"}


def test_an_unreadable_subreddit_is_reported_and_skipped(sqlite_db, wire_reddit, caplog):
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


def test_posts_writes_the_ingest_timestamp_on_every_row(sqlite_db, wire_reddit):
    wire_reddit(FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE)]))

    extract_posts()

    rows = sqlite_db.execute("SELECT ingested_at FROM r_posts").fetchall()
    assert len(rows) == 1
    assert rows[0][0] is not None  # ingested_at, stamped at load time


def test_empty_window_writes_nothing(sqlite_db, wire_reddit):
    wire_reddit(FakeSubreddit([FakeSubmission("stale", created_utc=OUTSIDE)]))

    written = extract_posts()

    assert written == 0
    assert sqlite_db.execute("SELECT count(*) FROM r_posts").fetchone()[0] == 0


def test_comment_values_round_trip_through_the_dump(sqlite_db, wire_reddit):
    # The payload-never-interpolated guarantee now lives in `dump_rows` (bound `?`
    # placeholders): a body that reads like SQL must land verbatim in the SQLite
    # dump, not be executed.
    body = "O'Brien; DROP TABLE workspace.topic_raw.r_comments; --"
    comment = FakeComment("c1", "p1", body, 5, FakeRedditor("alice"), INSIDE)
    wire_reddit(FakeSubreddit([FakeSubmission("p1", created_utc=INSIDE, comments=[comment])]))

    written = extract_comments()

    assert written == 1
    rows = sqlite_db.execute("SELECT body FROM r_comments").fetchall()
    assert [row[0] for row in rows] == [body]


def test_users_skips_deleted_authors_and_duplicates(sqlite_db, wire_reddit):
    posts = [
        FakeSubmission("p1", author=FakeRedditor("alice"), created_utc=INSIDE),
        FakeSubmission("p2", author=FakeRedditor("alice"), created_utc=INSIDE),  # duplicate name
        FakeSubmission("p3", author=None, created_utc=INSIDE),  # deleted author
    ]
    wire_reddit(FakeSubreddit(posts))

    written = extract_users()

    assert written == 1
    rows = sqlite_db.execute("SELECT username FROM r_users").fetchall()
    assert [row[0] for row in rows] == ["alice"]


def test_users_reports_unreadable_profiles_instead_of_swallowing_them(sqlite_db, wire_reddit, caplog):
    wire_reddit(FakeSubreddit([FakeSubmission("p1", author=BrokenRedditor(), created_utc=INSIDE)]))

    with caplog.at_level(logging.WARNING):
        written = extract_users()

    assert written == 0
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("ghost" in message for message in warnings)
