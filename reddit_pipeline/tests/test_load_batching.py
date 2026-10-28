"""Statement shape of the incremental loaders.

The loaders used to send one statement per row, which cost seconds of warehouse
time per row; these tests pin the batching that replaced it, and the bounds that
keep a batch inside DBSQL's parameter limit.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

from ingestion.reddit import _common
from tests.fakes import FakeCursor

COLUMNS: OrderedDict[str, str] = OrderedDict([("id", "STRING"), ("body", "STRING")])
TABLE = _common.full_table("r_comments")
ROW_WIDTH = len(COLUMNS)


def value_groups(statement: str) -> int:
    """Count the top-level `(...)` value tuples of an `insert ... values` statement."""
    body = statement.split(" VALUES ", 1)[1]
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


def row(index: int, body: str = "text") -> tuple[Any, ...]:
    return (f"c{index}", body)


def test_rows_are_batched_into_multi_row_statements() -> None:
    cursor = FakeCursor()

    written = _common.replace_by_key(cursor, TABLE, COLUMNS, [row(index) for index in range(120)], "id")

    assert written == 120
    # The whole point of the batching: 120 rows must not become 120 round trips.
    assert len(cursor.inserts) == 3
    assert [value_groups(sql) for sql, _ in cursor.inserts] == [50, 50, 20]
    assert [len(params) for _, params in cursor.inserts] == [50 * ROW_WIDTH, 50 * ROW_WIDTH, 20 * ROW_WIDTH]


def test_a_rolling_window_insert_is_batched_too() -> None:
    cursor = FakeCursor()

    _common.replace_window(cursor, TABLE, COLUMNS, [row(index) for index in range(60)], "created_utc", 0.0)

    assert [value_groups(sql) for sql, _ in cursor.inserts] == [50, 10]


def test_each_batch_stays_inside_the_parameter_budget() -> None:
    """A row budget alone overflows DBSQL's limit on a long comment body."""
    cursor = FakeCursor()
    body = "x" * (_common.INSERT_CHUNK_CHARS // 3)
    rows = [row(index, body) for index in range(7)]

    written = _common.replace_by_key(cursor, TABLE, COLUMNS, rows, "id")

    assert written == 7
    # ~256 KB of body per row against a ~768 KB budget: two rows fit, a third
    # does not, so the rows spread over four statements instead of two.
    assert [value_groups(sql) for sql, _ in cursor.inserts] == [2, 2, 2, 1]
    for _, params in cursor.inserts:
        assert len(params) % ROW_WIDTH == 0
        assert sum(len(value) for value in params if isinstance(value, str)) <= _common.INSERT_CHUNK_CHARS


def test_batches_bind_every_value() -> None:
    cursor = FakeCursor()
    rows = [row(index, "O'Brien; DROP TABLE x; --") for index in range(3)]

    _common.replace_by_key(cursor, TABLE, COLUMNS, rows, "id")

    statement, params = cursor.inserts[0]
    assert statement.count("?") == len(rows) * ROW_WIDTH
    assert params[:ROW_WIDTH] == rows[0]
