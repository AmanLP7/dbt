"""Contract between the Python insert path and the Delta raw table.

These tests keep the two halves of the raw table honest: the column list the
extractor binds against, the DDL that creates the table, and the statement
shape that makes a few hundred postings affordable to load.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from ingestion.jobs import _common
from tests.fakes import FakeCursor, raw_row

ROW_WIDTH = len(_common.RAW_COLUMNS)


def value_groups(statement: str) -> int:
    """Count the top-level `(...)` value tuples of an `insert ... values` statement."""
    body = statement.split(" values ", 1)[1]
    depth = 0
    groups = 0
    for char in body:
        if char == "(":
            depth += 1
            if depth == 1:
                groups += 1
        elif char == ")":
            depth -= 1
    return groups


def test_insert_packs_rows_into_chunked_statements() -> None:
    cursor = FakeCursor()
    rows = [raw_row(source_job_id=str(index)) for index in range(120)]

    written = _common.insert_postings(cursor, rows)

    assert written == 120
    # The whole point of the chunking: 120 rows must not become 120 round trips.
    assert len(cursor.inserts) == 3
    assert [value_groups(sql) for sql, _ in cursor.inserts] == [50, 50, 20]
    assert [len(params) for _, params in cursor.inserts] == [50 * ROW_WIDTH, 50 * ROW_WIDTH, 20 * ROW_WIDTH]


def test_insert_bounds_each_chunk_by_parameter_size() -> None:
    """A row budget alone overflows DBSQL's parameter limit on real descriptions."""
    cursor = FakeCursor()
    description = "x" * (_common.INSERT_CHUNK_CHARS // 3)
    rows = [raw_row(source_job_id=str(index), description=description) for index in range(7)]

    written = _common.insert_postings(cursor, rows)

    assert written == 7
    # ~256 KB of description per row against a ~768 KB budget: two rows fit, a
    # third does not, so the rows spread over four statements instead of two.
    assert [value_groups(sql) for sql, _ in cursor.inserts] == [2, 2, 2, 1]
    for _, params in cursor.inserts:
        assert len(params) % ROW_WIDTH == 0
        assert sum(len(value) for value in params if isinstance(value, str)) <= _common.INSERT_CHUNK_CHARS


def test_insert_binds_values_instead_of_interpolating_them() -> None:
    cursor = FakeCursor()
    row = raw_row(
        source="jobicy",
        source_job_id="42",
        title="Data'); drop table raw_job_postings --",
        company_name="Acme",
    )

    _common.insert_postings(cursor, [row])

    ((sql, params),) = cursor.inserts
    assert sql.startswith(_common._INSERT_PREFIX.format(fqn=_common.raw_table_fqn()))
    assert value_groups(sql) == 1
    assert "drop table" not in sql, "values are bound as parameters, never formatted into the statement"
    assert params[:-1] == tuple(row[column] for column in _common.RAW_COLUMNS[:-1])
    assert isinstance(params[-1], datetime), "the trailing parameter is the ingest stamp"


def test_insert_with_nothing_to_write_issues_no_statement() -> None:
    cursor = FakeCursor()

    assert _common.insert_postings(cursor, []) == 0
    assert cursor.statements == []


def test_every_row_of_a_run_shares_one_naive_utc_stamp() -> None:
    cursor = FakeCursor()
    before = _common.utcnow()

    _common.insert_postings(cursor, [raw_row(source_job_id=str(index)) for index in range(3)])

    after = _common.utcnow()
    stamps: list[Any] = [params[ROW_WIDTH - 1 :: ROW_WIDTH] for _, params in cursor.inserts]
    flat = [stamp for chunk in stamps for stamp in chunk]
    assert len(flat) == 3
    assert all(isinstance(stamp, datetime) for stamp in flat)
    assert set(flat) == {flat[0]}, "one run stamps one ingest time"
    assert before <= flat[0] <= after
    # The connector rejects tz-aware datetimes, so the stamp must stay naive.
    assert flat[0].tzinfo is None


def test_raw_columns_agree_across_ddl_placeholder_and_insert_prefix() -> None:
    body = _common._CREATE_TABLE_SQL.split("(", 1)[1].split(") using delta")[0]
    declared = [line.strip().split()[0] for line in body.strip().splitlines() if line.strip()]

    assert declared == list(_common.RAW_COLUMNS)
    assert _common.RAW_COLUMNS[-1] == "ingested_at"
    assert "(" + ", ".join(["?"] * ROW_WIDTH) + ")" == _common._ROW_PLACEHOLDER
    expected_prefix = "insert into c.s.t (" + ", ".join(_common.RAW_COLUMNS) + ") values "
    assert _common._INSERT_PREFIX.format(fqn="c.s.t") == expected_prefix


def test_raw_table_name_is_overridable_by_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOB_POSTINGS_CATALOG", raising=False)
    monkeypatch.delenv("JOB_POSTINGS_SCHEMA", raising=False)
    assert _common.raw_table_fqn() == "workspace.job_postings.raw_job_postings"

    monkeypatch.setenv("JOB_POSTINGS_CATALOG", "sandbox")
    monkeypatch.setenv("JOB_POSTINGS_SCHEMA", "jobs_dev")

    assert _common.raw_table_fqn() == "sandbox.jobs_dev.raw_job_postings"


def test_ensure_raw_table_creates_the_schema_before_the_table() -> None:
    cursor = FakeCursor()
    fqn = _common.raw_table_fqn()

    _common.ensure_raw_table(cursor)

    assert [sql for sql, _ in cursor.statements] == [
        f"create schema if not exists {fqn.rsplit('.', 1)[0]}",
        _common._CREATE_TABLE_SQL.format(fqn=fqn),
    ]
