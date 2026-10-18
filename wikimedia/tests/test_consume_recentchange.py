"""Tests for the recentchange SSE consumer.

Two seams keep this offline and deterministic: `iter_sse_frames` takes a plain
iterable of lines (no HTTP), and `consume` takes the cursor, the sleep function
and the stop predicate as arguments. Nothing here reaches Wikimedia or Databricks.

Every `consume` test passes `max_events`, because the real loop reconnects to a
live stream forever — a finite fake would otherwise be replayed endlessly.
"""

from __future__ import annotations

import json

import pytest
import requests

from ingestion.wikimedia import consume_recentchange as cr
from ingestion.wikimedia._common import RAW_COLUMNS, append_rows

EVENT_A = '{"meta":{"id":"aaa","dt":"2026-10-16T15:42:05.833Z"},"title":"A","wiki":"enwiki"}'
EVENT_B = '{"meta":{"id":"bbb","dt":"2026-10-16T15:42:06.833Z"},"title":"B","wiki":"dewiki"}'
EVENT_C = '{"meta":{"id":"ccc","dt":"2026-10-16T15:42:07.833Z"},"title":"C","wiki":"frwiki"}'

ID_A = '[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1,"offset":10}]'
ID_B = '[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":2,"offset":11}]'
ID_C = '[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":3,"offset":12}]'


def message(event_id: str, payload: str) -> list[str]:
    """The lines one EventStreams `message` frame occupies."""
    return [f"id: {event_id}", "event: message", f"data: {payload}", ""]


class FakeStream:
    """A stand-in for the `requests` streaming response."""

    def __init__(self, lines: list[str], error: Exception | None = None):
        self._lines = lines
        self._error = error

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def iter_lines(self, decode_unicode: bool = False):
        yield from self._lines
        if self._error is not None:
            raise self._error


class FakeCursor:
    """Records every statement executed, so the test can assert on the SQL shape."""

    def __init__(self, journal: list[tuple[str, object]] | None = None):
        self.journal = journal if journal is not None else []

    def execute(self, sql: str, params=None):
        self.journal.append(("insert", (sql, list(params or []))))


@pytest.fixture(autouse=True)
def checkpoint_file(tmp_path, monkeypatch):
    """Point the resume checkpoint at a temp file so tests never touch the repo."""
    path = tmp_path / "last_event_id.json"
    monkeypatch.setenv("WIKIMEDIA_STATE_FILE", str(path))
    return path


def run_consume(monkeypatch, streams, cursor, **kwargs):
    """Drive `consume` against a scripted sequence of streams."""
    opened = []

    def fake_open_stream(last_event_id, read_timeout):
        opened.append(last_event_id)
        return streams[min(len(opened) - 1, len(streams) - 1)]

    monkeypatch.setattr(cr, "open_stream", fake_open_stream)
    stats = cr.consume(cursor, backoffs=(0.0,), sleep=lambda _d: None, **kwargs)
    return stats, opened


def payloads_of(cursor):
    """Every payload written, in insert order (params are payload, timestamp, …)."""
    return [payload for _kind, (_sql, params) in cursor.journal for payload in params[0::2]]


def rows_per_statement(cursor):
    """How many rows each recorded INSERT carried."""
    return [len(params) // len(RAW_COLUMNS) for _kind, (_sql, params) in cursor.journal]


def test_parses_frames_heartbeats_and_multiline_data():
    lines = [
        ":ok",  # the stream's heartbeat comment
        "",
        *message(ID_A, EVENT_A),
        "data: {",
        'data: "split"',
        "data: }",
        "",
    ]

    frames = list(cr.iter_sse_frames(lines))

    assert frames[0] is None, "the heartbeat must surface as a tick, not a frame"
    assert frames[1] == cr.Frame(event_id=ID_A, event="message", data=EVENT_A)
    assert frames[2] == cr.Frame(event_id=None, event=None, data='{\n"split"\n}')


def test_tolerates_crlf_and_ignores_an_undispatched_final_frame():
    """A frame only ends at a blank line, so a truncated tail is dropped."""
    lines = [f"data: {EVENT_A}\r", "\r", f"data: {EVENT_B}"]

    frames = list(cr.iter_sse_frames(lines))

    assert frames == [cr.Frame(event_id=None, event=None, data=EVENT_A)]


def test_heartbeat_without_buffer_is_not_a_frame():
    assert list(cr.iter_sse_frames([":ok", ""])) == [None]


def test_appends_rows_and_never_mutates(monkeypatch):
    """The raw layer is insert-only: every flush is an INSERT and nothing else."""
    cursor = FakeCursor()
    lines = [*message(ID_A, EVENT_A), *message(ID_B, EVENT_B), *message(ID_C, EVENT_C)]

    stats, opened = run_consume(monkeypatch, [FakeStream(lines)], cursor, flush_every=2, max_events=3)

    assert stats == {"events": 3, "rows": 3, "flushes": 2, "reconnects": 0}
    assert len(cursor.journal) == 2, "two batches: 2 rows at the threshold, 1 on drain"

    for kind, (sql, _params) in cursor.journal:
        assert kind == "insert"
        assert sql.startswith(f"INSERT INTO workspace.wikimedia.raw_wikimedia ({', '.join(RAW_COLUMNS)}) VALUES ")
    assert rows_per_statement(cursor) == [2, 1]
    assert opened == [None], "a first run has no checkpoint and sends no Last-Event-ID"
    assert payloads_of(cursor) == [EVENT_A, EVENT_B, EVENT_C], "payloads land verbatim and in order"


def test_checkpoint_trails_the_insert(monkeypatch):
    """The checkpoint is written only after its rows are committed."""
    journal: list[tuple[str, object]] = []
    cursor = FakeCursor(journal)
    monkeypatch.setattr(cr, "write_checkpoint", lambda event_id: journal.append(("checkpoint", event_id)))

    lines = [*message(ID_A, EVENT_A), *message(ID_B, EVENT_B), *message(ID_C, EVENT_C)]
    run_consume(monkeypatch, [FakeStream(lines)], cursor, flush_every=2, max_events=3)

    assert [kind for kind, _ in journal] == ["insert", "checkpoint", "insert", "checkpoint"]
    assert [value for kind, value in journal if kind == "checkpoint"] == [ID_B, ID_C]


def test_resumes_from_the_checkpoint(monkeypatch, checkpoint_file):
    checkpoint_file.write_text(json.dumps({"last_event_id": ID_B}))
    cursor = FakeCursor()

    _stats, opened = run_consume(monkeypatch, [FakeStream(message(ID_C, EVENT_C))], cursor, flush_every=1, max_events=1)

    assert opened == [ID_B], "the resume id is sent as Last-Event-ID on the next connect"
    assert json.loads(checkpoint_file.read_text())["last_event_id"] == ID_C


def test_drops_non_json_frames(monkeypatch):
    cursor = FakeCursor()
    lines = [*message(ID_A, EVENT_A), *message(ID_B, "{not json"), *message(ID_C, EVENT_C)]

    stats, _opened = run_consume(monkeypatch, [FakeStream(lines)], cursor, flush_every=10, max_events=2)

    assert stats["events"] == 2, "the unparsable frame is dropped, not counted as an event"
    assert payloads_of(cursor) == [EVENT_A, EVENT_C]


def test_reconnects_and_replays_from_the_last_committed_id(monkeypatch):
    cursor = FakeCursor()
    dropped = FakeStream(message(ID_A, EVENT_A), error=requests.ConnectionError("boom"))
    resumed = FakeStream([*message(ID_B, EVENT_B), *message(ID_C, EVENT_C)])

    stats, opened = run_consume(monkeypatch, [dropped, resumed], cursor, flush_every=1, max_events=3)

    assert stats == {"events": 3, "rows": 3, "flushes": 3, "reconnects": 1}
    assert opened == [None, ID_A], "the reconnect resumes from the last committed event id"
    assert payloads_of(cursor) == [EVENT_A, EVENT_B, EVENT_C], "no event is lost across the drop"


def test_flushes_on_the_heartbeat_tick(monkeypatch):
    """A quiet stream still lands its rows: the tick flushes what the clock owes."""
    cursor = FakeCursor()
    lines = [*message(ID_A, EVENT_A), ":ok", "", *message(ID_B, EVENT_B)]

    stats, _opened = run_consume(
        monkeypatch, [FakeStream(lines)], cursor, flush_every=1000, flush_seconds=0.0, max_events=2
    )

    assert stats["flushes"] == 2, "one flush on the tick, one on the final drain"
    assert stats["rows"] == 2


def test_stops_without_losing_the_buffer(monkeypatch):
    cursor = FakeCursor()
    lines = [*message(ID_A, EVENT_A), *message(ID_B, EVENT_B)]

    stats, opened = run_consume(monkeypatch, [FakeStream(lines)], cursor, flush_every=100, should_stop=lambda: True)

    assert stats["events"] == 0
    assert opened == [], "a stop flag set before the run never opens the stream"
    assert cursor.journal == [], "nothing is written when the stop flag is already set"


def test_stops_after_max_events(monkeypatch):
    cursor = FakeCursor()
    lines = [*message(ID_A, EVENT_A), *message(ID_B, EVENT_B), *message(ID_C, EVENT_C)]

    stats, _opened = run_consume(monkeypatch, [FakeStream(lines)], cursor, max_events=2, flush_every=100)

    assert stats["events"] == 2
    assert stats["rows"] == 2
    assert payloads_of(cursor) == [EVENT_A, EVENT_B]


def test_append_rows_sends_one_statement_per_chunk():
    """Why `append_rows` avoids `executemany`: one round trip per chunk of rows."""
    cursor = FakeCursor()
    rows = [(f'{{"n":{i}}}', "2026-10-16T00:00:00") for i in range(5)]

    written = append_rows(cursor, "workspace.wikimedia.raw_wikimedia", RAW_COLUMNS, rows, chunk_size=2)

    assert written == 5
    assert len(cursor.journal) == 3, "5 rows in chunks of 2 is three statements, not five"
    assert rows_per_statement(cursor) == [2, 2, 1]
    assert cursor.journal[0][1][0].count("?") == 4, "both rows travel in one statement"
    assert payloads_of(cursor) == [row[0] for row in rows]


def test_append_rows_is_a_noop_for_no_rows():
    cursor = FakeCursor()

    assert append_rows(cursor, "workspace.wikimedia.raw_wikimedia", RAW_COLUMNS, []) == 0
    assert cursor.journal == [], "an empty batch must not issue a statement"
