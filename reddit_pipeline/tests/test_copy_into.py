"""The COPY INTO loader and its NDJSON export helpers.

The extractors leave rows in a local SQLite dump; `copy_into_raw` is what turns that
dump into warehouse rows: it exports each table to NDJSON, uploads the files to the
staging volume, and lands them with `COPY INTO`. The SQL helpers (`copy_selection`,
`ndjson_line`, `copy_into_statement`) are unit-tested directly, `dump_rows`/
`export_ndjson` against a real in-memory SQLite connection, and the loader itself
end-to-end against a real SQLite dump plus a recording Databricks cursor and volume.
"""

from __future__ import annotations

import json
import sqlite3
from collections import OrderedDict

import pytest

from ingestion.reddit import _common
from ingestion.reddit.copy_into import copy_into_raw
from tests.fakes import FakeCursor, FakeVolumeFiles

# One full-width row per raw table, in column order, with `ingested_at` already the
# string `dump_rows` would have written for a datetime.
POST_ROW = (
    "p1",  # id STRING
    "a title",  # title STRING
    10,  # score INT
    "https://example.invalid/post",  # url STRING
    3,  # comms_num INT
    1_700_000_000.0,  # created_utc DOUBLE
    "",  # body STRING
    "[deleted]",  # author STRING
    "worldnews",  # subreddit STRING
    "2026-11-13 12:00:00",  # ingested_at TIMESTAMP
)

COMMENT_ROW = (
    "c1",  # id STRING
    "p1",  # post_id STRING
    "a body",  # body STRING
    5,  # score INT
    "alice",  # author STRING
    1_700_000_000.0,  # created_utc DOUBLE
    "t3_parent",  # parent_id STRING
    "worldnews",  # subreddit STRING
    "2026-11-13 12:00:00",  # ingested_at TIMESTAMP
)

USER_ROW = (
    "alice",  # username STRING
    "t2_alice",  # id STRING
    1_700_000_000.0,  # created_utc DOUBLE
    1,  # link_karma INT
    2,  # comment_karma INT
    0,  # is_employee BOOLEAN
    0,  # is_gold BOOLEAN
    "2026-11-13 12:00:00",  # ingested_at TIMESTAMP
)


def test_copy_selection_casts_non_string_columns_and_leaves_string_bare() -> None:
    selection = _common.copy_selection(_common.POSTS_COLUMNS)

    # STRING columns travel bare, in backticks, with no cast.
    assert "`id`" in selection
    assert "CAST(`id`" not in selection
    assert "`title`" in selection
    assert "CAST(`title`" not in selection

    # Non-STRING columns are cast to their declared type, so COPY INTO parses the
    # JSON value as the table's type rather than defaulting to a string.
    assert "CAST(`score` AS INT) AS `score`" in selection
    assert "CAST(`comms_num` AS INT) AS `comms_num`" in selection
    assert "CAST(`created_utc` AS DOUBLE) AS `created_utc`" in selection
    assert "CAST(`ingested_at` AS TIMESTAMP) AS `ingested_at`" in selection

    user_selection = _common.copy_selection(_common.USERS_COLUMNS)
    assert "CAST(`is_employee` AS BOOLEAN) AS `is_employee`" in user_selection
    assert "CAST(`is_gold` AS BOOLEAN) AS `is_gold`" in user_selection


def test_ndjson_line_emits_valid_json_with_typed_values() -> None:
    columns: OrderedDict[str, str] = OrderedDict(
        [
            ("id", "STRING"),
            ("score", "INT"),
            ("created_utc", "DOUBLE"),
            ("is_employee", "BOOLEAN"),
            ("ingested_at", "TIMESTAMP"),
        ]
    )
    row = ("p1", 42, 1_700_000_000.5, 1, "2026-11-13 12:00:00")

    record = json.loads(_common.ndjson_line(columns, row))

    assert record["id"] == "p1"
    assert record["score"] == 42
    assert record["created_utc"] == 1_700_000_000.5
    assert record["is_employee"] is True  # JSON true, not the integer 1
    assert record["ingested_at"] == "2026-11-13 12:00:00"  # the datetime travelled as a string


def test_ndjson_line_does_not_escape_non_ascii() -> None:
    columns: OrderedDict[str, str] = OrderedDict([("body", "STRING")])

    line = _common.ndjson_line(columns, ("café 🎉",))

    # `ensure_ascii=False` writes the characters as UTF-8, not \u escapes.
    assert "café 🎉".encode() in line


def test_copy_into_statement_shape() -> None:
    table = _common.full_table("r_posts")
    source = "/Volumes/workspace/topic_raw/staging/load_20261113T000000Z/r_posts"

    statement = _common.copy_into_statement(table, source, _common.POSTS_COLUMNS)

    assert statement.startswith(f"COPY INTO {table}")
    assert "FROM (SELECT" in statement
    assert f"FROM '{source}'" in statement
    assert "FILEFORMAT = JSON" in statement
    assert "COPY_OPTIONS ('force' = 'false')" in statement
    assert _common.copy_selection(_common.POSTS_COLUMNS) in statement


def test_dump_rows_writes_rows_and_replace_clears_first() -> None:
    con = sqlite3.connect(":memory:")
    columns: OrderedDict[str, str] = OrderedDict([("id", "STRING"), ("title", "STRING")])
    _common.ensure_sqlite_table(con, "scratch", columns)

    written = _common.dump_rows(con, "scratch", columns, [("p1", "one"), ("p2", "two")])

    assert written == 2
    assert con.execute("SELECT id FROM scratch ORDER BY id").fetchall() == [("p1",), ("p2",)]

    # replace=True clears the table first, so a second dump overwrites, not appends.
    _common.dump_rows(con, "scratch", columns, [("p3", "three")], replace=True)
    assert con.execute("SELECT id FROM scratch").fetchall() == [("p3",)]


def test_export_ndjson_writes_one_line_per_row(tmp_path) -> None:
    con = sqlite3.connect(":memory:")
    columns: OrderedDict[str, str] = OrderedDict([("id", "STRING"), ("score", "INT")])
    _common.ensure_sqlite_table(con, "scratch", columns)
    _common.dump_rows(con, "scratch", columns, [("p1", 1), ("p2", 2), ("p3", 3)])

    exported = list(_common.export_ndjson(con, "scratch", columns, tmp_path / "scratch"))

    assert len(exported) == 1
    exported_file = exported[0]
    assert exported_file.rows == 3
    assert exported_file.size > 0
    assert exported_file.path == tmp_path / "scratch" / "part-00000.json"
    lines = exported_file.path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in lines] == ["p1", "p2", "p3"]


def test_ensure_columns_adds_the_missing_ingested_column() -> None:
    # A table created before `ingested_at` existed: DESCRIBE reports the old shape.
    legacy_columns = [name for name in _common.COMMENTS_COLUMNS if name != "ingested_at"]
    cursor = FakeCursor(described_columns=legacy_columns)

    _common.ensure_columns(cursor, _common.full_table("r_comments"), _common.COMMENTS_COLUMNS)

    assert cursor.alters == [f"ALTER TABLE {_common.full_table('r_comments')} ADD COLUMNS (ingested_at TIMESTAMP);"]


def test_ensure_columns_leaves_a_current_table_alone() -> None:
    cursor = FakeCursor()  # describes every column across the three tables

    _common.ensure_columns(cursor, _common.full_table("r_comments"), _common.COMMENTS_COLUMNS)

    assert cursor.alters == []


def test_copy_into_raw_uploads_and_copies_each_non_empty_table(sqlite_db, copy_warehouse, tmp_path) -> None:
    _common.dump_rows(sqlite_db, "r_posts", _common.POSTS_COLUMNS, [POST_ROW])
    _common.dump_rows(sqlite_db, "r_comments", _common.COMMENTS_COLUMNS, [COMMENT_ROW])
    # r_users is left empty: the loader must skip its COPY INTO.

    volume_files = FakeVolumeFiles()
    result = copy_into_raw(
        db_path=tmp_path / "reddit.db",
        volume_files=volume_files,
        work_dir=tmp_path / "export",
        keep_files=True,
    )

    assert result.rows["r_posts"] == 1
    assert result.rows["r_comments"] == 1
    assert result.rows.get("r_users", 0) == 0
    assert result.files == 2

    # One COPY INTO per non-empty table, none for the empty one.
    copy_statements = [sql for sql, _ in copy_warehouse.statements if sql.startswith("COPY INTO")]
    assert len(copy_statements) == 2
    assert sum("r_posts" in sql for sql in copy_statements) == 1
    assert sum("r_comments" in sql for sql in copy_statements) == 1
    assert sum("r_users" in sql for sql in copy_statements) == 0

    # Every table, empty or not, has its target ensured before the COPY INTO lands.
    for table in ("r_posts", "r_comments", "r_users"):
        assert f"CREATE TABLE IF NOT EXISTS {_common.full_table(table)}" in copy_warehouse.sql_text
        assert f"DESCRIBE TABLE {_common.full_table(table)}" in copy_warehouse.sql_text

    # Each non-empty table's file was uploaded under its own run-dir subdirectory.
    assert {remote.rsplit("/", 1)[0].rsplit("/", 1)[1] for remote, _ in volume_files.uploads} == {
        "r_posts",
        "r_comments",
    }

    # keep_files=True leaves the run dir in place and records it.
    assert result.run_dir is not None
    assert result.run_dir.startswith("/Volumes/workspace/topic_raw/staging/load_")
    assert volume_files.removed == []


def test_copy_into_raw_clears_stale_staging_and_removes_the_run_dir(sqlite_db, copy_warehouse, tmp_path) -> None:
    _common.dump_rows(sqlite_db, "r_posts", _common.POSTS_COLUMNS, [POST_ROW])

    stale = "/Volumes/workspace/topic_raw/staging/load_20261112T000000Z"
    volume_files = FakeVolumeFiles(dirs=[stale])

    result = copy_into_raw(
        db_path=tmp_path / "reddit.db",
        volume_files=volume_files,
        work_dir=tmp_path / "export",
        keep_files=False,
    )

    # The stale dir is discovered by prefix and removed, then the run's own dir too.
    assert volume_files.list_calls == [("/Volumes/workspace/topic_raw/staging", _common.STAGING_PREFIX)]
    assert stale in volume_files.removed
    assert len(volume_files.removed) == 2  # stale dir, then this run's dir
    assert result.run_dir is None


def test_copy_into_raw_dry_run_reports_counts_without_touching_databricks(sqlite_db, copy_warehouse, tmp_path) -> None:
    _common.dump_rows(sqlite_db, "r_posts", _common.POSTS_COLUMNS, [POST_ROW])

    result = copy_into_raw(db_path=tmp_path / "reddit.db", dry_run=True)

    assert result.rows == {"r_posts": 1, "r_comments": 0, "r_users": 0}
    assert copy_warehouse.statements == []


def test_copy_into_raw_raises_without_a_dump(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        copy_into_raw(db_path=tmp_path / "missing.db")


def test_copy_into_raw_restricts_to_the_requested_tables(sqlite_db, copy_warehouse, tmp_path) -> None:
    # All three dumps hold rows, but only r_posts is selected: the other two must be
    # left untouched even though their SQLite tables are non-empty.
    _common.dump_rows(sqlite_db, "r_posts", _common.POSTS_COLUMNS, [POST_ROW])
    _common.dump_rows(sqlite_db, "r_comments", _common.COMMENTS_COLUMNS, [COMMENT_ROW])
    _common.dump_rows(sqlite_db, "r_users", _common.USERS_COLUMNS, [USER_ROW])

    volume_files = FakeVolumeFiles()
    result = copy_into_raw(
        db_path=tmp_path / "reddit.db",
        volume_files=volume_files,
        work_dir=tmp_path / "export",
        keep_files=True,
        tables=("r_posts",),
    )

    assert result.rows == {"r_posts": 1}
    assert result.files == 1

    # One COPY INTO, only for the selected table; the others are never even ensured.
    copy_statements = [sql for sql, _ in copy_warehouse.statements if sql.startswith("COPY INTO")]
    assert len(copy_statements) == 1
    assert sum("r_posts" in sql for sql in copy_statements) == 1
    assert sum("r_comments" in sql for sql in copy_statements) == 0
    assert sum("r_users" in sql for sql in copy_statements) == 0

    assert f"CREATE TABLE IF NOT EXISTS {_common.full_table('r_posts')}" in copy_warehouse.sql_text
    assert f"CREATE TABLE IF NOT EXISTS {_common.full_table('r_comments')}" not in copy_warehouse.sql_text
    assert f"CREATE TABLE IF NOT EXISTS {_common.full_table('r_users')}" not in copy_warehouse.sql_text
