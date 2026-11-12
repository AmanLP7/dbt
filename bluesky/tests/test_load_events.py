"""Statement shape of the batched load, and the resume logic behind it.

The whole point of the loader is that a backlog costs a handful of round trips
instead of one per row; these tests pin the batching, the two DBSQL caps that
bound it, the watermark that makes a re-run idempotent, and the guards that stop
a silent gap from being reported as a clean run.
"""

from __future__ import annotations

import sqlite3

import pytest

from ingestion.bluesky import _common, load_events
from tests.fakes import FakeWarehouse, event_row


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


def test_the_plan_and_the_batcher_agree_on_row_size(source_db) -> None:
    """`pending_stats` and `row_chars` must price a row identically.

    They are two implementations of one number — SQL for the plan, Python for the
    batch cap — and a disagreement mis-prices the run or splits batches at the wrong
    place. Non-ASCII is the case that separates bytes from characters.
    """
    rows = [
        event_row(0),
        event_row(1, payload='{"text": "héllo 🌍 wörld"}'),
        event_row(2, payload="x" * 5_000),
        event_row(3, partition=1),
    ]
    path = source_db(rows)
    con = _common.connect_sqlite(path)

    count, chars = _common.pending_stats(con, {})
    batched = [row for batch in _common.iter_batches(con, {}, batch_rows=10) for row in batch]
    con.close()

    assert count == 4
    assert chars == sum(_common.row_chars(row) for row in batched)


def test_batches_respect_the_dbsql_parameter_cap() -> None:
    """769 rows of thirteen columns is the most one statement may carry."""
    assert _common.MAX_STATEMENT_PARAMETERS // len(_common.RAW_COLUMNS) == _common.MAX_ROWS_PER_STATEMENT
    assert _common.MAX_ROWS_PER_STATEMENT * len(_common.RAW_COLUMNS) <= 10_000
    # One more row would breach it, which is why the cap is derived, not chosen.
    assert (_common.MAX_ROWS_PER_STATEMENT + 1) * len(_common.RAW_COLUMNS) > 10_000


def test_a_backlog_becomes_one_statement_per_batch(source_db, warehouse: FakeWarehouse) -> None:
    """1,550 rows at the 769-row cap: two full batches and a remainder."""
    path = source_db([event_row(offset) for offset in range(1550)])

    result = load_events.load_events(db_path=path)

    assert result.rows == 1550
    assert result.statements == 3
    assert [value_groups(sql) for sql, _ in warehouse.cursor_obj.inserts] == [769, 769, 12]
    assert len(warehouse.cursor_obj.inserted()) == 1550


def test_the_character_cap_binds_before_the_row_cap_on_large_payloads(source_db, warehouse: FakeWarehouse) -> None:
    """A payload-heavy batch must not exceed 1 MiB of bound text."""
    payload = "y" * 100_000
    path = source_db([event_row(offset, payload=payload) for offset in range(12)])

    result = load_events.load_events(db_path=path)

    assert result.rows == 12
    # ~100 KB a row against a ~768 KB budget: seven rows fit, an eighth does not.
    assert [value_groups(sql) for sql, _ in warehouse.cursor_obj.inserts] == [7, 5]
    for _, params in warehouse.cursor_obj.inserts:
        assert sum(len(value) for value in params if isinstance(value, str)) <= _common.INSERT_CHUNK_CHARS


def test_an_oversized_row_goes_alone_rather_than_being_truncated(source_db, warehouse: FakeWarehouse) -> None:
    """A single row past the whole budget is still written whole."""
    big = "z" * (_common.INSERT_CHUNK_CHARS + 10)
    path = source_db([event_row(0), event_row(1, payload=big), event_row(2)])

    load_events.load_events(db_path=path)

    assert [value_groups(sql) for sql, _ in warehouse.cursor_obj.inserts] == [1, 1, 1]
    payload_index = list(_common.RAW_COLUMNS).index("payload")
    written = {row[_common.OFFSET_INDEX]: row[payload_index] for row in warehouse.cursor_obj.inserted()}
    assert written[1] == big


def test_insert_binds_every_value(source_db, warehouse: FakeWarehouse) -> None:
    """Values are parameters, never interpolated text."""
    path = source_db([event_row(0, payload="O'Brien; DROP TABLE x; --")])

    load_events.load_events(db_path=path)

    statement, params = warehouse.cursor_obj.inserts[0]
    assert statement.count("?") == len(params) == len(_common.RAW_COLUMNS)
    # The payload travels as a bound parameter, so the quote and the semicolons
    # are data, not SQL.
    assert params[-1] == "O'Brien; DROP TABLE x; --"


def test_the_resume_point_comes_from_the_target(source_db, warehouse: FakeWarehouse) -> None:
    """A second run re-reads only what the target has not seen."""
    path = source_db([event_row(offset) for offset in range(20)])
    first = load_events.load_events(db_path=path, batch_rows=10, limit=10)

    assert first.rows == 10
    assert first.watermarks == {0: 9}

    warehouse.cursor_obj.execute("SELECT `partition`, MAX(`offset`) FROM t GROUP BY `partition`;")
    warehouse.cursor_obj._watermarks = {0: 9}
    second = load_events.load_events(db_path=path, batch_rows=10)

    assert second.rows == 10
    offsets = [row[_common.OFFSET_INDEX] for row in warehouse.cursor_obj.inserted()[10:]]
    assert min(offsets) == 10 and max(offsets) == 19


def test_a_fully_loaded_source_issues_no_insert(source_db, warehouse: FakeWarehouse) -> None:
    path = source_db([event_row(offset) for offset in range(5)])
    warehouse.cursor_obj._watermarks = {0: 4}

    result = load_events.load_events(db_path=path)

    assert result.rows == 0
    assert warehouse.cursor_obj.inserts == []


def test_each_partition_resumes_independently(source_db, warehouse: FakeWarehouse) -> None:
    """Partitions keep separate watermarks, so a slow one is not skipped."""
    rows = [event_row(offset, partition=0) for offset in range(5)]
    rows += [event_row(offset, partition=1) for offset in range(5)]
    path = source_db(rows)
    warehouse.cursor_obj._watermarks = {0: 2}

    result = load_events.load_events(db_path=path)

    offsets = [(row[_common.PARTITION_INDEX], row[_common.OFFSET_INDEX]) for row in warehouse.cursor_obj.inserted()]
    assert offsets == [(0, 3), (0, 4), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4)]
    assert result.watermarks == {0: 4, 1: 4}


def test_a_retention_gap_is_reported_not_swallowed(source_db, warehouse: FakeWarehouse, caplog) -> None:
    """A target watermark below the source's first offset is a hole, and says so."""
    path = source_db([event_row(offset) for offset in range(100, 105)])
    warehouse.cursor_obj._watermarks = {0: 10}

    load_events.load_events(db_path=path)

    assert "Gap on partition 0" in caplog.text
    assert "11-99" in caplog.text


def test_rows_of_another_topic_are_ignored(source_db, warehouse: FakeWarehouse) -> None:
    path = source_db([event_row(0), event_row(1, topic="other.topic")])

    result = load_events.load_events(db_path=path)

    assert result.rows == 1
    assert warehouse.cursor_obj.inserted()[0][_common.OFFSET_INDEX] == 0


def test_a_batch_does_not_pin_the_wal_between_statements(source_db) -> None:
    """The caller's INSERT must run with no SQLite read snapshot outstanding.

    The source is a live WAL database under the Flink sink, and a `sqlite3` cursor
    left mid-iteration holds a read transaction for as long as the caller takes —
    6 s per batch, 4 h for the backlog, during which the sink's ~0.4 MB/s of writes
    cannot be checkpointed and the `-wal` file grows without bound. The check here
    is the mechanism itself: between batches, a checkpoint must not report busy.
    """
    path = source_db([event_row(offset) for offset in range(25)])
    writer = sqlite3.connect(path)
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    # A stand-in for the sink's own writes. It must not be `events` itself: page
    # reads resume past the last row, so rows arriving mid-run would extend the
    # scan, and the loader bounds a run by the backlog it planned for exactly
    # because of that.
    writer.execute("CREATE TABLE sink_writes (at INTEGER PRIMARY KEY, payload TEXT)")
    writer.commit()
    writer.close()

    con = _common.connect_sqlite(path)
    writer = sqlite3.connect(path)  # the Flink sink, writing meanwhile
    checkpoints: list[tuple[int, int]] = []
    for index, _batch in enumerate(_common.iter_batches(con, {}, batch_rows=10)):
        # Stand in for the warehouse INSERT: the sink commits while we are paused.
        writer.execute("INSERT INTO sink_writes VALUES (?, ?)", (index, "x" * 4000))
        writer.commit()
        busy, log_frames, _checkpointed = writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        checkpoints.append((busy, log_frames))
    con.close()
    writer.close()

    assert len(checkpoints) == 3
    # busy == 0 means no reader was holding the snapshot; log_frames == 0 means the
    # WAL was fully truncated.
    assert checkpoints == [(0, 0), (0, 0), (0, 0)]


def test_assume_empty_is_refused_on_a_real_run(source_db, warehouse: FakeWarehouse) -> None:
    """It would append the whole source a second time, and nothing dedupes."""
    path = source_db([event_row(0)])

    with pytest.raises(SystemExit):
        load_events.main(["--db", str(path), "--assume-empty"])
    with pytest.raises(ValueError, match="dry run"):
        load_events.load_events(db_path=path, assume_empty=True)
    assert warehouse.cursor_obj.statements == []


def test_a_run_loads_the_backlog_it_planned_and_then_stops(
    source_db, warehouse: FakeWarehouse, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live sink must not turn a drain into an unbounded chase.

    Page reads resume past the last row, so rows the sink writes while the load runs
    would be picked up too — and the warehouse absorbs ~191 rows/s against a firehose
    of ~450/s, so that loop never ends. The run is bounded by the count its own plan
    printed: 20 rows here, with 20 more arriving before the first INSERT returns.
    """
    path = source_db([event_row(offset) for offset in range(20)])
    sink = sqlite3.connect(path)

    real_insert = _common.insert_batch
    written = iter(range(100, 400, 10))

    def insert_and_sink(cursor, table, rows):
        # A fresh slice per batch: `seq` is the primary key, so a repeat would be a
        # constraint error rather than a second sink write.
        start = next(written)
        sink.executemany(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [event_row(offset, seq=90_000_000 + offset) for offset in range(start, start + 10)],
        )
        sink.commit()
        return real_insert(cursor, table, rows)

    monkeypatch.setattr(_common, "insert_batch", insert_and_sink)

    result = load_events.load_events(db_path=path, batch_rows=10)
    sink.close()

    assert result.rows == 20
    assert [row[_common.OFFSET_INDEX] for row in warehouse.cursor_obj.inserted()] == list(range(20))


def test_a_missing_database_fails_loudly(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        _common.connect_sqlite(tmp_path / "absent.db")


def test_the_database_is_opened_read_only(source_db) -> None:
    """A writer connection would take the lock the Flink sink needs."""
    con = _common.connect_sqlite(source_db([event_row(0)]))

    with pytest.raises(sqlite3.OperationalError):
        con.execute("DELETE FROM events")
    con.close()


def test_a_row_limit_stops_the_run_cleanly(source_db, warehouse: FakeWarehouse) -> None:
    path = source_db([event_row(offset) for offset in range(50)])

    result = load_events.load_events(db_path=path, batch_rows=10, limit=10)

    assert result.rows == 10
    assert result.stopped_early == "row limit of 10 reached"
    assert result.watermarks == {0: 9}


def test_dry_run_writes_nothing(source_db, warehouse: FakeWarehouse, caplog) -> None:
    """Not even the DDL: a dry run must not create the table it is pricing."""
    path = source_db([event_row(offset) for offset in range(30)])

    result = load_events.load_events(db_path=path, dry_run=True)

    assert result.rows == 0
    assert warehouse.cursor_obj.inserts == []
    # Nothing but reads: no CREATE (ensure_raw_table runs after the dry-run branch),
    # no INSERT, no MERGE.
    assert warehouse.cursor_obj.statements, "a dry run still reads the target"
    for sql, _ in warehouse.cursor_obj.statements:
        assert sql.lstrip().upper().startswith("SELECT"), sql
    assert "Plan: 30 rows pending" in caplog.text


def test_an_empty_source_is_not_an_error(source_db, warehouse: FakeWarehouse, caplog) -> None:
    path = source_db([])

    result = load_events.load_events(db_path=path)

    assert result.rows == 0
    assert "nothing to load" in caplog.text
