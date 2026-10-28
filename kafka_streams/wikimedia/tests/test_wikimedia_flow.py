"""Offline tests for the Wikimedia Kafka flow.

No broker, no HTTP, no Wikimedia. The seams are the pure functions
(`parse_event`, `event_row`, `iter_ndjson`, `project`), `write_batch` against a
temporary SQLite file, and `consume` driven by a fake consumer that only needs
the methods the loop calls.
"""

from __future__ import annotations

import json
import logging
import socket
import sqlite3
from pathlib import Path

import pytest
from ingestion.wikimedia import consumer as cs
from ingestion.wikimedia import flink_job as fj
from ingestion.wikimedia import producer as pr
from ingestion.wikimedia._common import (
    EVENT_COLUMNS,
    connect_db,
    ensure_schema,
    heal_cursor,
    parse_event,
    stored_offset,
    write_batch,
)
from ingestion.wikimedia.consumer import start_offset

EVENT_A = {
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
        "id": "aaa",
        "dt": "2026-10-20T18:00:00Z",
        "domain": "en.wikipedia.org",
        "topic": "eqiad.mediawiki.recentchange",
    },
    "id": 1,
    "type": "edit",
    "title": "A",
    "user": "Alice",
    "bot": False,
    "wiki": "enwiki",
}
EVENT_B = {
    **EVENT_A,
    "meta": {**EVENT_A["meta"], "id": "bbb", "dt": "2026-10-20T18:00:01Z"},
    "title": "B",
    "wiki": "dewiki",
}
CANARY = {**EVENT_A, "meta": {**EVENT_A["meta"], "id": "canary", "domain": "canary"}}
NO_ID = {**EVENT_A, "meta": {**EVENT_A["meta"], "id": None}}


@pytest.fixture
def db(tmp_path):
    connection = connect_db(tmp_path / "events.db")
    ensure_schema(connection)
    yield connection
    connection.close()


def row_for(event: dict, offset: int = 0) -> tuple:
    row = cs.event_row(json.dumps(event), "2026-10-20T18:01:00Z", 0, offset)
    assert row is not None  # a test fixture event must map to a row
    return row


# --- parse_event ------------------------------------------------------------


def test_parse_event_keeps_real_events():
    event = parse_event(json.dumps(EVENT_A))
    assert event is not None
    assert event["meta"]["id"] == "aaa"


def test_parse_event_drops_canary_and_id_less_events():
    assert parse_event(json.dumps(CANARY)) is None
    assert parse_event(json.dumps(NO_ID)) is None


def test_parse_event_drops_malformed_lines():
    assert parse_event("not json") is None
    assert parse_event("[]") is None
    assert parse_event('{"meta": "not-an-object"}') is None


def test_parse_event_survives_undecodable_bytes():
    """One non-UTF-8 message must not kill the sink; it is dropped, not fatal."""
    assert parse_event(b"\xff\xfe not json") is None
    assert parse_event(b'{"meta": {"id": "x\xff"}}') is not None


def test_parse_event_rejects_a_non_string_id():
    """The id becomes a Kafka key; an integer id would raise inside the producer."""
    assert parse_event(json.dumps({**EVENT_A, "meta": {**EVENT_A["meta"], "id": 1}})) is None


# --- producer seam ----------------------------------------------------------


def test_iter_ndjson_ignores_blank_keepalives():
    lines = ["", "  ", '{"a": 1}', '["not", "an", "object"]', '{"b": 2}']
    assert list(pr.iter_ndjson(lines)) == ['{"a": 1}', '["not", "an", "object"]', '{"b": 2}']


def test_project_returns_the_bytes_wikimedia_sent():
    raw = json.dumps(EVENT_A)
    assert pr.project(raw) == (raw, EVENT_A)
    assert pr.project(json.dumps(CANARY)) is None


# --- SQLite sink ------------------------------------------------------------


def test_event_row_maps_meta_and_kafka_position():
    row = dict(zip(tuple(EVENT_COLUMNS), row_for(EVENT_A, offset=41), strict=True))
    assert row["event_id"] == "aaa"
    assert row["event_dt"] == "2026-10-20T18:00:00Z"
    assert row["topic"] == "wikimedia.recentchange"
    assert row["partition"] == 0
    assert row["offset"] == 41
    assert row["wiki"] == "enwiki"
    assert row["bot"] == 0
    assert json.loads(row["payload"]) == EVENT_A


def test_write_batch_inserts_rows_and_offset_together(db):
    changed = write_batch(db, [row_for(EVENT_A, 10)], {("wikimedia.recentchange", 0): 11})

    assert changed == 2  # one event insert + one offset upsert
    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert stored_offset(db, "wikimedia.recentchange", 0) == 11


def test_write_batch_replay_is_a_noop_for_rows_but_advances_offset(db):
    write_batch(db, [row_for(EVENT_A, 10)], {("wikimedia.recentchange", 0): 11})
    changed = write_batch(db, [row_for(EVENT_A, 10)], {("wikimedia.recentchange", 0): 12})

    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert stored_offset(db, "wikimedia.recentchange", 0) == 12, "the offset still advances over a duplicate row"
    assert changed == 1, "only the offset upsert changed anything"


def test_write_batch_records_the_cluster_id_beside_the_offset(db):
    write_batch(db, [row_for(EVENT_A, 10)], {("wikimedia.recentchange", 0): 11}, cluster_id="cluster-a")

    assert db.execute("SELECT cluster_id FROM kafka_offsets").fetchone()[0] == "cluster-a"


def test_write_batch_rolls_back_rows_when_the_offset_update_fails(db):
    bad_checkpoint: dict[tuple[str, int], int] = {("wikimedia.recentchange", 0): None}  # type: ignore[dict-item]
    with pytest.raises(sqlite3.IntegrityError):  # next_offset is NOT NULL
        write_batch(db, [row_for(EVENT_A, 10)], bad_checkpoint)

    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert stored_offset(db, "wikimedia.recentchange", 0) is None


# --- producer loop ----------------------------------------------------------


class FakeProducer:
    """Records what was produced and resolves each delivery, like librdkafka does."""

    def __init__(self, delivery_error=None):
        self.records: list[tuple[str, bytes | None, bytes | None]] = []
        self.delivery_error = delivery_error
        self.flushes: list[float] = []

    def produce(self, topic, value=None, key=None, on_delivery=None):
        self.records.append((topic, value, key))
        if on_delivery is not None:
            on_delivery(self.delivery_error, self.records[-1])

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        self.flushes.append(timeout)
        return 0


class FakeResponse:
    """A stream that yields its lines, then optionally raises like a dropped socket."""

    def __init__(self, lines, error=None):
        self._lines = lines
        self._error = error

    def iter_lines(self, decode_unicode=False):
        yield from self._lines
        if self._error is not None:
            raise self._error

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Clock:
    """Deterministic monotonic clock: each call returns the next listed value."""

    def __init__(self, values):
        self._values = list(values)
        self._index = 0

    def __call__(self):
        value = self._values[min(self._index, len(self._values) - 1)]
        self._index += 1
        return value


def test_producer_reconnects_after_a_drop_without_losing_events(monkeypatch):
    line_a = json.dumps(EVENT_A)
    line_b = json.dumps(EVENT_B)
    streams = [FakeResponse([line_a], error=pr.requests.ConnectionError("boom")), FakeResponse([line_b])]
    monkeypatch.setattr(pr, "open_stream", lambda timeout: streams.pop(0))

    producer = FakeProducer()
    slept: list[float] = []
    stats = pr.bridge(producer, max_events=2, should_stop=lambda: False, sleep=slept.append)

    assert stats["events"] == 2
    assert stats["reconnects"] == 1
    assert stats["delivered"] == 2, "both events were acknowledged by the broker"
    assert stats["failed"] == 0
    assert [record[2] for record in producer.records] == [b"aaa", b"bbb"], "the resumed stream is not lost"
    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0]], "a dropped stream backs off from the start of the ladder"


def test_producer_reconnects_when_the_server_closes_the_stream(monkeypatch):
    """Wikimedia closes healthy connections at 15 minutes; that must not end the run."""
    lines = [json.dumps(EVENT_A)] * 20
    streams = [FakeResponse(lines), FakeResponse(lines)]
    monkeypatch.setattr(pr, "open_stream", lambda timeout: streams.pop(0))

    producer = FakeProducer()
    slept: list[float] = []
    # `connected_at` is 0; each line check then costs one tick, so line 14 breaks
    # the stream. The second connection resumes from the top of the same buffer.
    stats = pr.bridge(
        producer,
        max_events=16,
        reconnect_seconds=14,
        clock=Clock(range(1000)),
        sleep=slept.append,
    )

    assert stats["reconnects"] == 1, "a timer close reconnects immediately, without the error ladder"
    assert stats["events"] == 16
    assert len(producer.records) == 16
    assert streams == [], "the run used a second connection rather than ending"
    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0]]


def test_producer_never_opens_the_stream_when_already_stopped(monkeypatch):
    opened: list[float] = []
    monkeypatch.setattr(pr, "open_stream", lambda timeout: opened.append(timeout) or FakeResponse([]))

    stats = pr.bridge(FakeProducer(), should_stop=lambda: True)

    assert stats["events"] == 0
    assert opened == [], "a stop flag set before the run must not open a connection"


def test_producer_counts_a_rejected_delivery_as_failed_not_delivered(monkeypatch):
    """Enqueued is not delivered: a broker rejection must not be counted as success."""
    monkeypatch.setattr(pr, "open_stream", lambda timeout: FakeResponse([json.dumps(EVENT_A)]))

    stats = pr.bridge(FakeProducer(delivery_error="UNKNOWN_TOPIC_OR_PART"), max_events=1)

    assert stats["enqueued"] == 1
    assert stats["delivered"] == 0
    assert stats["failed"] == 1


class OverflowingProducer(FakeProducer):
    """Raises like librdkafka's full local queue, after recording the message."""

    def produce(self, topic, value=None, key=None, on_delivery=None):
        super().produce(topic, value=value, key=key, on_delivery=on_delivery)
        raise BufferError("Local: Queue full")


def test_producer_flushes_the_queue_when_produce_raises(monkeypatch):
    """A crash mid-stream must not abandon events already handed to the client."""
    monkeypatch.setattr(pr, "open_stream", lambda timeout: FakeResponse([json.dumps(EVENT_A)]))

    producer = OverflowingProducer()
    with pytest.raises(BufferError):
        pr.bridge(producer)

    assert producer.flushes == [30], "the queue is flushed on the way out"
    assert len(producer.records) == 1


# --- consumer loop ----------------------------------------------------------


class FakeMessage:
    def __init__(self, payload: dict | str | bytes, offset: int, partition: int = 0):
        if isinstance(payload, bytes):
            self._value = payload
        else:
            self._value = (payload if isinstance(payload, str) else json.dumps(payload)).encode()
        self._offset = offset
        self._partition = partition

    def error(self):
        return None

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return "wikimedia.recentchange"


class FakeConsumer:
    """Returns queued messages, then None forever, and records nothing else."""

    def __init__(self, messages):
        self._messages = list(messages)

    def poll(self, timeout):
        return self._messages.pop(0) if self._messages else None


def test_consume_writes_batches_and_advances_offsets(db):
    messages = [FakeMessage(EVENT_A, 100), FakeMessage(CANARY, 101), FakeMessage(EVENT_B, 102)]
    stats = cs.consume(db, FakeConsumer(messages), max_events=3, flush_every=2)

    assert stats["messages"] == 3
    assert stats["rows"] == 2
    assert stats["skipped"] == 1
    rows = db.execute("SELECT event_id FROM events ORDER BY event_id").fetchall()
    assert [r[0] for r in rows] == ["aaa", "bbb"]
    # The canary still moves the offset, or the consumer would re-read it forever.
    assert stored_offset(db, "wikimedia.recentchange", 0) == 103


def test_consume_writes_the_final_batch_when_stopped(db):
    """A signal flipped mid-buffer must still commit what was read."""
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 2

    messages = [FakeMessage(EVENT_A, 7), FakeMessage(EVENT_B, 8)]
    stats = cs.consume(db, FakeConsumer(messages), flush_every=100, should_stop=should_stop)

    assert stats["rows"] == 2
    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
    assert stored_offset(db, "wikimedia.recentchange", 0) == 9


def test_consume_stops_on_max_seconds_without_messages(db):
    stats = cs.consume(db, FakeConsumer([]), max_seconds=0)
    assert stats["messages"] == 0
    assert stats["flushes"] == 0


def test_consume_commits_an_undecodable_message_as_skipped(db):
    """The stalled-sink bug: a bad message must advance the offset, not crash the run."""
    stats = cs.consume(db, FakeConsumer([FakeMessage(b"\xff\xfe not utf8", 5)]), max_events=1)

    assert stats["skipped"] == 1
    assert stored_offset(db, "wikimedia.recentchange", 0) == 6


def test_consume_stores_an_event_with_one_bad_byte(db):
    """A valid event carrying an invalid byte is stored, not raised on."""
    payload = b'{"meta": {"id": "ccc"}, "title": "caf\xff"}'
    stats = cs.consume(db, FakeConsumer([FakeMessage(payload, 5)]), max_events=1)

    assert stats["rows"] == 1
    stored = db.execute("SELECT event_id, payload FROM events").fetchone()
    assert stored[0] == "ccc"
    assert stored[1] == '{"meta": {"id": "ccc"}, "title": "caf\ufffd"}'
    assert stored_offset(db, "wikimedia.recentchange", 0) == 6


# --- resume position --------------------------------------------------------


@pytest.mark.parametrize(
    ("stored", "low", "high", "expected"),
    [
        (None, 0, 300, 0),  # fresh database: read the retained backlog
        (120, 0, 300, 120),  # normal resume
        (0, 0, 300, 0),  # stored offset 0 is a real position, not "unset"
        (10, 50, 300, 50),  # retention deleted the stored offset
        (400, 0, 300, 0),  # topic recreated: stored offset is past the end
        (0, 0, 0, 0),  # empty topic: wait at the start
    ],
)
def test_start_offset_clamps_to_the_log(stored, low, high, expected):
    assert start_offset(stored, low, high) == expected


class FakeCluster:
    """The three `confluent_kafka.Consumer` methods `assign_partitions` calls."""

    def __init__(self, low: int = 0, high: int = 300, cluster_id: str | None = "cluster-a"):
        self.low = low
        self.high = high
        self.cluster_id = cluster_id
        self.assigned: list = []

    def list_topics(self, topic=None, timeout=None):
        from types import SimpleNamespace

        partition = SimpleNamespace(id=0)
        return SimpleNamespace(
            cluster_id=self.cluster_id,
            topics={topic: SimpleNamespace(partitions={0: partition})},
        )

    def get_watermark_offsets(self, partition, timeout=None):
        return self.low, self.high

    def assign(self, partitions):
        self.assigned = partitions


def test_assign_partitions_resumes_from_sqlite(db):
    write_batch(db, [], {("wikimedia.recentchange", 0): 120}, cluster_id="cluster-a")
    cluster = FakeCluster(low=0, high=300)

    assignment, cluster_id = cs.assign_partitions(db, cluster, "wikimedia.recentchange")

    assert [tp.offset for tp in assignment] == [120]
    assert cluster.assigned == assignment
    assert cluster_id == "cluster-a", "the cluster id is returned so the checkpoint can record it"


def test_assign_partitions_refuses_a_retention_gap(db):
    write_batch(db, [], {("wikimedia.recentchange", 0): 10}, cluster_id="cluster-a")

    with pytest.raises(SystemExit, match="retention"):
        cs.assign_partitions(db, FakeCluster(low=50, high=300), "wikimedia.recentchange")


def test_assign_partitions_accepts_a_retention_gap_when_told_to(db):
    write_batch(db, [], {("wikimedia.recentchange", 0): 10}, cluster_id="cluster-a")

    assignment, _ = cs.assign_partitions(db, FakeCluster(low=50, high=300), "wikimedia.recentchange", allow_gap=True)

    assert [tp.offset for tp in assignment] == [50]


def test_assign_partitions_logs_a_first_run_without_a_checkpoint(db, caplog):
    """An empty checkpoint used to fall into the clamp branch and crash the log call."""
    caplog.set_level(logging.INFO, logger="ingestion")

    assignment, _ = cs.assign_partitions(db, FakeCluster(low=7, high=300), "wikimedia.recentchange")

    assert [tp.offset for tp in assignment] == [7]
    assert "no stored offset" in caplog.text
    assert "outside the log" not in caplog.text


def test_assign_partitions_resets_when_the_cluster_changed(db):
    """A recreated broker reuses offsets from 0, so the old checkpoint can land mid-log."""
    write_batch(db, [], {("wikimedia.recentchange", 0): 120}, cluster_id="old-cluster")

    assignment, cluster_id = cs.assign_partitions(db, FakeCluster(low=0, high=300), "wikimedia.recentchange")

    assert [tp.offset for tp in assignment] == [0]
    assert cluster_id == "cluster-a"


def test_database_path_resolves_a_relative_override_against_the_flow(monkeypatch):
    """A relative override must not depend on the shell's cwd."""
    from ingestion.wikimedia import _common

    monkeypatch.setenv("WIKIMEDIA_SQLITE_PATH", "data/other.db")
    assert _common.database_path() == _common.PROJECT_ROOT / "data" / "other.db"

    monkeypatch.setenv("WIKIMEDIA_SQLITE_PATH", "/tmp/abs.db")
    assert _common.database_path() == _common.Path("/tmp/abs.db")


# --- Flink job seams --------------------------------------------------------

# `flink_job` is importable without PyFlink: the pyflink imports are inside
# `run()`, so only the SQL and the start-offset planning are testable here.


def test_flink_column_types_offsets_as_bigint():
    """`events.offset` is the column `heal_cursor` aggregates; INT overflows at 2^31."""
    assert fj.flink_column("offset", "INTEGER") == "BIGINT"
    assert fj.flink_column("partition", "INTEGER") == "INT"
    assert fj.flink_column("event_id", "TEXT PRIMARY KEY") == "STRING"
    assert fj.flink_column("payload", "TEXT NOT NULL") == "STRING"
    assert fj.flink_column("bot", "INTEGER") == "INT"


def test_dashboard_port_skips_a_port_already_in_use():
    """A pinned port only helps if it moves when something else holds the first."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        busy = taken.getsockname()[1]

        assert fj.dashboard_port(busy, span=4) != busy


def test_dashboard_port_reports_exhaustion():
    """Every port busy must mean None (random port), not a crash."""
    sockets = []
    try:
        for port in range(39200, 39204):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", port))
            sock.listen(1)
            sockets.append(sock)
        assert fj.dashboard_port(39200, span=4) is None
    finally:
        for sock in sockets:
            sock.close()


def test_flink_column_refuses_an_unmapped_type():
    """Defaulting to INT would truncate a future REAL column on every write, silently."""
    with pytest.raises(ValueError, match="REAL"):
        fj.flink_column("score", "REAL")


def test_source_ddl_positions_the_consumer_at_the_stored_offsets():
    ddl = fj.source_ddl("localhost:9092", {("wikimedia.recentchange", 0): 620}, bounded=False)

    assert "'scan.startup.mode' = 'specific-offsets'" in ddl
    assert "'scan.startup.specific-offsets' = 'partition:0,offset:620'" in ddl
    assert "'format' = 'raw'" in ddl
    assert "bounded" not in ddl, "an unbounded run must not stop at the log end"
    for metadata in ("`partition` INT METADATA FROM 'partition' VIRTUAL", "`offset` BIGINT METADATA FROM 'offset'"):
        assert metadata in ddl


def test_source_ddl_stops_at_the_log_end_only_when_bounded():
    ddl = fj.source_ddl("localhost:9092", {("wikimedia.recentchange", 0): 0}, bounded=True)
    assert "'scan.bounded.mode' = 'latest-offset'" in ddl


def test_sink_ddl_lists_every_events_column_and_the_sqlite_url(tmp_path):
    ddl = fj.sink_ddl(tmp_path / "events.db")

    for column, sql_type in EVENT_COLUMNS.items():
        assert f"`{column}` {fj.flink_column(column, sql_type)}" in ddl
    assert f"'url' = 'jdbc:sqlite:{tmp_path / 'events.db'}'" in ddl
    assert "'table-name' = 'events'" in ddl


def test_sink_ddl_absolutises_a_relative_database_path(tmp_path, monkeypatch):
    """The JVM resolves a relative `jdbc:sqlite:` path against its own cwd, not the shell's."""
    monkeypatch.chdir(tmp_path)

    ddl = fj.sink_ddl(Path("nested/events.db"))

    assert f"'url' = 'jdbc:sqlite:{tmp_path / 'nested' / 'events.db'}'" in ddl


def test_connect_db_accepts_the_string_that_db_flag_passes(tmp_path):
    """`--db` is a str; `connect_db` used to call `.parent` on it and crash."""
    db = connect_db(str(tmp_path / "cli.db"))
    ensure_schema(db)
    db.close()

    assert (tmp_path / "cli.db").exists()


def test_insert_sql_maps_the_columns_in_events_order():
    """The SELECT list is positional: a swap writes wiki names into `title`."""
    sql = fj.insert_sql()

    assert "?" not in sql, "this is a Flink INSERT ... SELECT; placeholders belong to the dialect"
    select_list = sql.split("FROM recentchange_source")[0]
    assert select_list.count("JSON_VALUE(payload, '$.") == 7, "six scalars plus the bot flag"
    # One distinctive fragment per column, in `EVENT_COLUMNS` order.
    projections = {
        "event_id": "JSON_VALUE(payload, '$.meta.id')",
        "ingested_at": "CONCAT(DATE_FORMAT(CURRENT_TIMESTAMP",
        "event_dt": "JSON_VALUE(payload, '$.meta.dt')",
        "topic": "'wikimedia.recentchange'",
        "partition": "`partition`",
        "offset": "`offset`",
        "wiki": "JSON_VALUE(payload, '$.wiki')",
        "type": "JSON_VALUE(payload, '$.type')",
        "title": "JSON_VALUE(payload, '$.title')",
        "user": "JSON_VALUE(payload, '$.user')",
        "bot": "CASE WHEN",
        "payload": "payload\n",
    }
    assert list(projections) == list(EVENT_COLUMNS), "the test's list must track the table"
    positions = [select_list.index(fragment) for fragment in projections.values()]
    assert positions == sorted(positions), "projection order must match EVENT_COLUMNS"


def test_insert_sql_drops_canary_and_id_less_events():
    """The WHERE clause is `parse_event` in SQL; both rejections must be there."""
    sql = fj.insert_sql()
    assert "JSON_VALUE(payload, '$.meta.id') <> ''" in sql
    assert "COALESCE(JSON_VALUE(payload, '$.meta.domain'), '') <> 'canary'" in sql


def test_jar_uris_names_every_missing_jar(tmp_path):
    with pytest.raises(SystemExit) as excinfo:
        fj.jar_uris(tmp_path)

    message = str(excinfo.value)
    for jar in fj.JARS:
        assert jar in message


def test_jar_uris_returns_absolute_file_uris(tmp_path):
    for jar in fj.JARS:
        (tmp_path / jar).write_bytes(b"")

    uris = fj.jar_uris(tmp_path)

    assert len(uris) == len(fj.JARS)
    assert all(uri.startswith("file:///") for uri in uris)


def test_heal_cursor_recovers_the_position_after_the_highest_row(db):
    """The Flink sink cannot write the offset with its rows, so it is derived."""
    write_batch(db, [row_for(EVENT_A, 10), row_for(EVENT_B, 11)], {})

    assert heal_cursor(db, "wikimedia.recentchange") == 1
    assert stored_offset(db, "wikimedia.recentchange", 0) == 12


def test_heal_cursor_never_moves_a_cursor_backwards(db):
    write_batch(db, [row_for(EVENT_A, 10)], {("wikimedia.recentchange", 0): 99})

    assert heal_cursor(db, "wikimedia.recentchange") == 0
    assert stored_offset(db, "wikimedia.recentchange", 0) == 99


def test_heal_cursor_is_a_noop_when_already_current(db):
    """An exact cursor (the Python consumer's) must not be rewritten on every start."""
    write_batch(db, [row_for(EVENT_A, 10)], {("wikimedia.recentchange", 0): 11})

    assert heal_cursor(db, "wikimedia.recentchange") == 0
    assert stored_offset(db, "wikimedia.recentchange", 0) == 11


def test_heal_cursor_ignores_an_empty_events_table(db):
    assert heal_cursor(db, "wikimedia.recentchange") == 0
    assert stored_offset(db, "wikimedia.recentchange", 0) is None


def test_heal_cursor_creates_the_checkpoint_row_it_has_no_deadlock_with(db):
    """The INSERT branch: rows but no `kafka_offsets` row, so `cluster_id` lands NULL."""
    write_batch(db, [row_for(EVENT_A, 10)], {})
    db.execute("DELETE FROM kafka_offsets")
    db.commit()

    assert heal_cursor(db, "wikimedia.recentchange") == 1
    assert stored_offset(db, "wikimedia.recentchange", 0) == 11
    cluster_id = db.execute("SELECT cluster_id FROM kafka_offsets").fetchone()[0]
    assert cluster_id is None, "a healed row knows no broker; the next start stamps the real one"
