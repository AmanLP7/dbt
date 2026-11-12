"""Statement shape of the incremental loaders.

The loaders used to send one statement per row, which cost seconds of warehouse
time per row; these tests pin the batching that replaced it, and the bounds that
keep a batch inside DBSQL's parameter limit.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

import pytest

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


def test_the_key_delete_is_chunked_too() -> None:
    """A run reads every seeded subreddit, so its key list is thousands long.

    A single `IN (...)` with thousands of parameters is one very large statement;
    the chunking keeps each statement's parameter list small, and every key still
    travels bound. The chunks are also what bounds the blast radius of a failed
    insert, since the delete and insert are interleaved per chunk.
    """
    cursor = FakeCursor()
    count = _common.DELETE_CHUNK_ROWS * 2 + 7

    _common.replace_by_key(cursor, TABLE, COLUMNS, [row(index) for index in range(count)], "id")

    assert [sql.count("?") for sql, _ in cursor.deletes] == [_common.DELETE_CHUNK_ROWS] * 2 + [7]
    bound = [key for _, params in cursor.deletes for key in params]
    assert bound == [f"c{index}" for index in range(count)]
    groups = [value_groups(sql) for sql, _ in cursor.inserts]
    assert sum(groups) == count
    assert max(groups) == _common.INSERT_CHUNK_ROWS


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


def test_the_parameter_budget_counts_utf8_bytes_not_characters() -> None:
    """Reddit bodies are user text; the server's limit is bytes, so an emoji is four."""
    cursor = FakeCursor()
    # 4 bytes per character. At this length two rows total 1 MiB of bound
    # parameters, so only one fits per statement — while the same *character*
    # count in ASCII would have packed five. A `len()`-based budget silently
    # overfills the statement and DBSQL rejects it.
    body = "🎉" * (_common.INSERT_CHUNK_CHARS // 6)
    rows = [row(index, body) for index in range(4)]

    _common.replace_by_key(cursor, TABLE, COLUMNS, rows, "id")

    assert [value_groups(sql) for sql, _ in cursor.inserts] == [1, 1, 1, 1]
    for _, params in cursor.inserts:
        bound = sum(len(value.encode()) for value in params if isinstance(value, str))
        assert bound <= _common.INSERT_CHUNK_CHARS
    # The same rows in ASCII pack several per statement; that is the difference
    # the byte count buys.
    ascii_chunks = _common.chunk_rows([row(index, "x" * len(body)) for index in range(4)])
    assert len(ascii_chunks) == 1


def test_a_failed_insert_cannot_strand_the_whole_run() -> None:
    """The delete and insert are separate autocommitted statements.

    Deleting every id first and inserting afterwards means one failed INSERT
    leaves the entire run deleted — and a daily 26-hour read cannot refetch rows
    older than that, so they would be gone for good. Interleaving by chunk bounds
    the loss to the chunk in flight.
    """
    cursor = FakeCursor()
    count = _common.DELETE_CHUNK_ROWS * 2 + 7
    rows = [row(index) for index in range(count)]

    # Fail the very first insert, after its chunk's delete has committed.
    def explode(operation: str, parameters: Any = None) -> Any:
        if operation.startswith("INSERT"):
            raise RuntimeError("connection reset")
        cursor.statements.append((operation, parameters))
        return cursor

    cursor.execute = explode  # type: ignore[method-assign]

    with pytest.raises(RuntimeError):
        _common.replace_by_key(cursor, TABLE, COLUMNS, rows, "id")

    # Exactly one chunk was deleted, not all three: the remaining ids still exist.
    assert len(cursor.deletes) == 1
    assert [len(params) for _, params in cursor.deletes] == [_common.DELETE_CHUNK_ROWS]


def test_batches_bind_every_value() -> None:
    cursor = FakeCursor()
    rows = [row(index, "O'Brien; DROP TABLE x; --") for index in range(3)]

    _common.replace_by_key(cursor, TABLE, COLUMNS, rows, "id")

    statement, params = cursor.inserts[0]
    assert statement.count("?") == len(rows) * ROW_WIDTH
    assert params[:ROW_WIDTH] == rows[0]
