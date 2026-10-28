"""Offline tests for the Bluesky Kafka flow.

No broker, no WebSocket, no Bluesky. The seams are the pure functions
(`parse_event`, `record_key`, `project`, `frame_age_seconds`), `write_batch` and
`heal_cursor` against a temporary SQLite file, `bridge` driven by a fake async
socket, the resume-position planner driven by a fake cluster, and the Flink DDL
strings.

`flink_job` is the flow's only reader, and no test here starts a JVM: the SQL it
generates is asserted as text, and everything that decides which messages survive
is exercised on the Python side, where the producer computes the fields the
sink reads.

Every frame constant below was copied from a live capture of
`jetstream.us-east.bsky.network` on 2026-10-27, with the ids shortened. The v2
envelope, the `#`-suffixed `$type`, and the absence of a `kind` field are the
three properties the parser exists for.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import socket
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from ingestion.bluesky import _common, _kafka
from ingestion.bluesky import flink_job as fj
from ingestion.bluesky import producer as pr
from ingestion.bluesky._common import (
    EVENT_COLUMNS,
    TOPIC,
    connect_db,
    ensure_schema,
    event_kind,
    heal_cursor,
    parse_event,
    record_key,
    save_cursor,
    stored_cursor,
    stored_offset,
    utc_now,
    write_batch,
)
from ingestion.bluesky._kafka import resolve_offsets, start_offset

COMMIT = {
    "$type": "network.bsky.jetstream.subscribeEvents#commit",
    "cid": "bafyreiabc",
    "collection": "app.bsky.feed.post",
    "did": "did:plc:aaaa",
    "operation": "create",
    "record": {"$type": "app.bsky.feed.post", "text": "hello"},
    "rev": "3kxyz",
    "rkey": "3kxyz",
    "seq": 26498700001,
    "time": "2026-10-27T15:00:00.000Z",
    "witnessedAt": "2026-10-27T15:00:00.100Z",
}
COMMIT_B = {
    **COMMIT,
    "did": "did:plc:bbbb",
    "rkey": "3kxy2",
    "seq": 26498700002,
    "time": "2026-10-27T15:00:01.000Z",
}
ACCOUNT = {
    "$type": "network.bsky.jetstream.subscribeEvents#account",
    "account": {"active": True, "did": "did:plc:aaaa", "seq": 34897371596, "time": "2026-10-27T15:00:02.000Z"},
    "did": "did:plc:aaaa",
    "seq": 26498700003,
    "time": "2026-10-27T15:00:02.500Z",
    "witnessedAt": "2026-10-27T15:00:02.600Z",
}
NO_SEQ = {key: value for key, value in COMMIT.items() if key != "seq"}


def wrapped(payload: dict) -> str:
    """The v2 envelope: every event arrives as `{"$type": "message", "payload": ...}`."""
    return json.dumps({"$type": "message", "payload": payload})


@pytest.fixture
def db(tmp_path):
    connection = connect_db(tmp_path / "events.db")
    ensure_schema(connection)
    yield connection
    connection.close()


def commit_time_us() -> int:
    """The fixture commit's event time in microseconds, or a hard failure."""
    value = pr.event_time_us(COMMIT)
    assert value is not None, "the fixture must carry a parsable `time`"
    return value


def project_or_fail(frame) -> tuple[str, dict]:
    """`project`, narrowed: the tests below only use frames it accepts."""
    record = pr.project(frame)
    assert record is not None, "the fixture must be a frame the producer accepts"
    return record


INGESTED_AT = "2026-10-27T15:01:00.000+00:00"


def row_for(event: dict, offset: int = 0, ingested_at: str = INGESTED_AT, partition: int = 0) -> tuple:
    """A `main.events` row for one event: the producer's body, projected as Flink does.

    The flow has one writer, so there is no Python function left that maps a
    message to a row -- `insert_sql()` is that mapping now. This fixture mirrors
    its SELECT list so `write_batch` and `heal_cursor` can be tested without a
    JVM; `test_insert_sql_maps_the_columns_in_events_order` pins the order, and
    `test_project_emits_a_body_the_flink_sink_will_keep` pins the values the
    filter depends on.
    """
    body, _payload = project_or_fail(wrapped(event))
    decoded = json.loads(body)
    return (
        decoded["record_key"],
        ingested_at,
        decoded.get("time"),
        decoded.get("kind"),
        decoded.get("collection"),
        decoded.get("did"),
        decoded.get("operation"),
        decoded.get("rkey"),
        decoded.get("seq"),
        TOPIC,
        partition,
        offset,
        body,
    )


# --- parse_event and the envelope -------------------------------------------


def test_parse_event_unwraps_the_v2_envelope():
    event = parse_event(wrapped(COMMIT))
    assert event is not None
    assert event["seq"] == COMMIT["seq"]


def test_parse_event_accepts_an_unenveloped_v1_frame():
    """v1 frames are the same payload without the wrapper; the parser takes both."""
    event = parse_event(json.dumps(COMMIT))
    assert event is not None
    assert event["did"] == "did:plc:aaaa"


def test_parse_event_drops_an_error_envelope():
    """A v2 error frame carries the reason, not an event, under the same `payload` key."""
    error = json.dumps({"$type": "error", "message": "Invalid cursor"})
    assert parse_event(error) is None


def test_parse_event_drops_events_without_an_integer_seq():
    assert parse_event(wrapped(NO_SEQ)) is None
    assert parse_event(wrapped({**COMMIT, "seq": "26498700001"})) is None
    assert parse_event(wrapped({**COMMIT, "seq": None})) is None


def test_parse_event_drops_malformed_frames():
    assert parse_event("not json") is None
    assert parse_event("[]") is None
    assert parse_event('{"$type": "message", "payload": "not-an-object"}') is None


def test_parse_event_survives_undecodable_bytes():
    """One non-UTF-8 frame must not kill the sink; it is dropped, not fatal."""
    assert parse_event(b"\xff\xfe not json") is None
    assert parse_event(json.dumps({**COMMIT, "rkey": "caf\ufffd"}).encode()) is not None


# --- identity ---------------------------------------------------------------


def test_event_kind_is_the_suffix_of_the_type():
    assert event_kind(COMMIT) == "commit"
    assert event_kind(ACCOUNT) == "account"
    assert event_kind({"$type": "no-hash"}) is None
    assert event_kind({}) is None


def test_record_key_uses_the_at_uri_for_a_commit():
    assert record_key(COMMIT) == "at://did:plc:aaaa/app.bsky.feed.post/3kxyz"


def test_record_key_uses_the_seq_for_an_account_event():
    """An account event names an account, not a record; only its `seq` is unique."""
    assert record_key(ACCOUNT) == "at://did:plc:aaaa/account/26498700003"


def test_record_key_is_none_without_a_did():
    assert record_key({**COMMIT, "did": None}) is None
    assert record_key({**ACCOUNT, "did": ""}) is None
    assert record_key({**COMMIT, "rkey": None}) is not None, "the seq fallback still applies"


# --- producer projection ----------------------------------------------------


def test_project_adds_kind_and_record_key_to_the_body():
    """Flink cannot right-split `$type` nor build an `at://` URI, so the body carries both."""
    body, payload = project_or_fail(wrapped(COMMIT))

    assert payload["seq"] == COMMIT["seq"]
    decoded = json.loads(body)
    assert decoded["kind"] == "commit"
    assert decoded["record_key"] == "at://did:plc:aaaa/app.bsky.feed.post/3kxyz"
    assert decoded["record"]["text"] == "hello", "nothing upstream is dropped"
    assert decoded["$type"].endswith("#commit"), "the original `$type` is kept"


def test_project_drops_frames_parse_event_would_drop():
    assert pr.project(wrapped(NO_SEQ)) is None
    assert pr.project("not json") is None


def test_every_body_the_producer_sends_satisfies_the_flink_filter():
    """The sink's WHERE clause is the only validity rule left, so the producer must feed it.

    `insert_sql()` drops a body whose `record_key` is empty or whose `seq` does
    not cast to an integer, and neither check can be exercised without a JVM.
    What ties the two halves together is that `project()` already drops those
    frames upstream: `parse_event` requires an integer `seq` and a non-None
    `record_key`. This asserts that seam, so a future relaxation of `parse_event`
    cannot start losing rows at the sink instead of at the producer.
    """
    accepted = [COMMIT, ACCOUNT, {**COMMIT, "rkey": None, "collection": None}]
    for event in accepted:
        body, _ = project_or_fail(wrapped(event))
        decoded = json.loads(body)
        assert isinstance(decoded.get("seq"), int), "a NULL seq would make SQLite invent a rowid"
        assert decoded.get("record_key"), "an empty record_key is NULL-false in the WHERE clause"

    for rejected in ({**COMMIT, "did": None}, NO_SEQ, {**COMMIT, "seq": "26498700001"}):
        assert pr.project(wrapped(rejected)) is None, "the sink would drop it; drop it here instead"


def test_frame_age_seconds_reads_the_event_time():
    now = 1_800_000_000.0
    stamp = datetime.datetime.fromtimestamp(now - 90.0, datetime.timezone.utc).isoformat()
    payload = {**COMMIT, "time": stamp.replace("+00:00", "Z")}

    assert pr.frame_age_seconds(payload, now) == pytest.approx(90.0, abs=0.01)
    assert pr.frame_age_seconds({**COMMIT, "time": "not a time"}, now) is None
    assert pr.frame_age_seconds({**COMMIT, "time": None}, now) is None


def test_event_time_us_is_microseconds_of_the_event_time():
    assert pr.event_time_us(COMMIT) == int(
        datetime.datetime(2026, 10, 27, 15, 0, 0, tzinfo=datetime.timezone.utc).timestamp() * 1_000_000
    )


def test_build_url_repeats_each_filter_value():
    """v2 repeats `collections=` per value; a comma-separated list is rejected at handshake."""
    url = pr.build_url("wss://example/x", cursor=26498700001, collections=("a.b", "c.d"), kinds=("commit",))

    assert url.startswith("wss://example/x?")
    assert "cursor=26498700001" in url
    assert "collections=a.b&collections=c.d" in url
    assert "kinds=commit" in url


def test_build_url_omits_an_empty_query():
    assert pr.build_url("wss://example/x", collections=(), kinds=()) == "wss://example/x"


# --- SQLite sink ------------------------------------------------------------


def test_write_batch_inserts_rows_and_offset_together(db):
    changed = write_batch(db, [row_for(COMMIT, 10)], {(TOPIC, 0): 11})

    assert changed == 2  # one event insert + one offset upsert
    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert stored_offset(db, TOPIC, 0) == 11


def test_write_batch_dedupes_a_replayed_event_on_its_seq(db):
    """At-least-once delivery: the second write of the same event must be a no-op row."""
    write_batch(db, [row_for(COMMIT, 10)], {(TOPIC, 0): 11})
    changed = write_batch(db, [row_for(COMMIT, 10)], {(TOPIC, 0): 12})

    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert stored_offset(db, TOPIC, 0) == 12, "the offset still advances over a duplicate row"
    assert changed == 1, "only the offset upsert changed anything"


def test_write_batch_keeps_a_second_event_for_the_same_record(db):
    """A `delete` after a `create` is a different event on the same `at://` URI.

    Measured on the first 3,500 live messages: 15 events (0.43%) were exactly
    this, and keying the table on the URI discarded them.
    """
    deleted = {**COMMIT, "operation": "delete", "seq": COMMIT["seq"] + 1, "time": "2026-10-27T15:00:01.000Z"}

    write_batch(db, [row_for(COMMIT, 10), row_for(deleted, 11)], {(TOPIC, 0): 12})

    rows = db.execute("SELECT operation FROM events ORDER BY seq").fetchall()
    assert [row[0] for row in rows] == ["create", "delete"]
    assert db.execute("SELECT COUNT(DISTINCT record_key) FROM events").fetchone()[0] == 1


def test_write_batch_records_the_cluster_id_beside_the_offset(db):
    write_batch(db, [row_for(COMMIT, 10)], {(TOPIC, 0): 11}, cluster_id="cluster-a")

    assert db.execute("SELECT cluster_id FROM kafka_offsets").fetchone()[0] == "cluster-a"


def test_write_batch_rolls_back_rows_when_the_offset_update_fails(db):
    bad_checkpoint: dict[tuple[str, int], int] = {(TOPIC, 0): None}  # type: ignore[dict-item]
    with pytest.raises(sqlite3.IntegrityError):  # next_offset is NOT NULL
        write_batch(db, [row_for(COMMIT, 10)], bad_checkpoint)

    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert stored_offset(db, TOPIC, 0) is None


def test_cursor_round_trips_and_moves_forward(db):
    assert stored_cursor(db) is None

    save_cursor(db, 1_793_115_058_872_458)
    assert stored_cursor(db) == 1_793_115_058_872_458

    save_cursor(db, 1_793_115_058_872_500)
    assert stored_cursor(db) == 1_793_115_058_872_500
    assert db.execute("SELECT COUNT(*) FROM jetstream_cursor").fetchone()[0] == 1, "one row per stream"


def test_heal_cursor_recovers_the_position_after_the_highest_row(db):
    """The Flink sink cannot write the offset with its rows, so it is derived."""
    write_batch(db, [row_for(COMMIT, 10), row_for(COMMIT_B, 11)], {})

    assert heal_cursor(db, TOPIC) == 1
    assert stored_offset(db, TOPIC, 0) == 12


def test_heal_cursor_never_moves_a_cursor_backwards(db):
    """Recovery is monotone: rows below an already-advanced checkpoint change nothing."""
    write_batch(db, [], {(TOPIC, 0): 99})
    write_batch(db, [row_for(COMMIT, 10)], {}, "cluster-a")

    assert heal_cursor(db, TOPIC, "cluster-a") == 0
    assert stored_offset(db, TOPIC, 0) == 99


def test_heal_cursor_is_a_noop_when_already_current(db):
    write_batch(db, [], {(TOPIC, 0): 11})
    write_batch(db, [row_for(COMMIT, 10)], {}, "cluster-a")

    assert heal_cursor(db, TOPIC, "cluster-a") == 0
    assert stored_offset(db, TOPIC, 0) == 11


def test_heal_cursor_ignores_an_empty_events_table(db):
    assert heal_cursor(db, TOPIC) == 0
    assert stored_offset(db, TOPIC, 0) is None


def test_heal_cursor_creates_the_checkpoint_row_it_has_no_deadlock_with(db):
    """The INSERT branch: rows but no `kafka_offsets` row."""
    write_batch(db, [row_for(COMMIT, 10)], {})

    assert heal_cursor(db, TOPIC) == 1
    assert stored_offset(db, TOPIC, 0) == 11


def test_heal_cursor_stamps_a_missing_cluster_id(db):
    """A NULL cluster would switch the recreated-broker check off for good."""
    write_batch(db, [row_for(COMMIT, 10)], {})

    assert heal_cursor(db, TOPIC, "cluster-a") == 1
    assert db.execute("SELECT cluster_id FROM kafka_offsets").fetchone()[0] == "cluster-a"


def test_heal_cursor_never_crosses_a_different_cluster(db):
    """Rows written against a replaced log must not push a live checkpoint past it."""
    write_batch(db, [], {(TOPIC, 0): 5}, "cluster-old")
    # A recreated broker: the new log starts behind the checkpoint, and these
    # rows are the new log's, ingested after it was stamped.
    write_batch(db, [row_for(COMMIT, 900, ingested_at=utc_now())], {}, "cluster-new")

    assert heal_cursor(db, TOPIC, "cluster-new") == 0
    assert stored_offset(db, TOPIC, 0) == 5, "the checkpoint belongs to the new log and must survive"


def test_heal_cursor_counts_only_rows_ingested_after_the_checkpoint(db):
    """An old log's highest offset must not be applied to a run that started later.

    Without the timestamp scope, a broker recreated behind a correct checkpoint
    at 0 would be jumped by the *previous* log's highest offset on the next
    start, skipping the entire fresh backlog.
    """
    write_batch(db, [row_for(COMMIT, 5000)], {}, "cluster-a")
    write_batch(db, [], {(TOPIC, 0): 0}, "cluster-a")
    write_batch(db, [row_for(COMMIT_B, 2, ingested_at=utc_now())], {}, "cluster-a")

    assert heal_cursor(db, TOPIC, "cluster-a") == 1
    assert stored_offset(db, TOPIC, 0) == 3, "only this run's rows count, not the whole table"


def test_heal_cursor_never_crosses_a_recreated_topic(db):
    """Same broker, new log: the cluster id matches and only the topic id sees it."""
    write_batch(db, [], {(TOPIC, 0): 5}, "cluster-a", "topic-old")
    write_batch(db, [row_for(COMMIT, 900, ingested_at=utc_now())], {}, "cluster-a", "topic-new")

    assert heal_cursor(db, TOPIC, "cluster-a", "topic-new") == 0
    assert stored_offset(db, TOPIC, 0) == 5


def test_heal_cursor_stamps_a_missing_topic_id(db):
    write_batch(db, [row_for(COMMIT, 10)], {})

    assert heal_cursor(db, TOPIC, "cluster-a", "topic-a") == 1
    assert db.execute("SELECT topic_id FROM kafka_offsets").fetchone()[0] == "topic-a"


def test_heal_cursor_heals_when_the_topic_id_matches(db):
    write_batch(db, [], {(TOPIC, 0): 5}, "cluster-a", "topic-a")
    write_batch(db, [row_for(COMMIT, 9, ingested_at=utc_now())], {}, "cluster-a", "topic-a")

    assert heal_cursor(db, TOPIC, "cluster-a", "topic-a") == 1
    assert stored_offset(db, TOPIC, 0) == 10


def test_write_batch_records_the_topic_id_beside_the_offset(db):
    write_batch(db, [], {(TOPIC, 0): 11}, cluster_id="cluster-a", topic_id="topic-a")

    assert db.execute("SELECT topic_id FROM kafka_offsets").fetchone()[0] == "topic-a"


def test_write_batch_never_blanks_a_recorded_topic_id(db):
    """A caller that could not read the id must not disarm the check."""
    write_batch(db, [], {(TOPIC, 0): 11}, cluster_id="cluster-a", topic_id="topic-a")

    write_batch(db, [], {(TOPIC, 0): 12}, cluster_id="cluster-a", topic_id=None)

    assert db.execute("SELECT topic_id FROM kafka_offsets").fetchone()[0] == "topic-a"


def test_ensure_schema_adds_the_topic_id_column_to_an_existing_sink(tmp_path):
    """Every deployed sink predates the column; CREATE TABLE IF NOT EXISTS skips it."""
    path = tmp_path / "old.db"
    legacy = sqlite3.connect(path)
    legacy.execute(
        "CREATE TABLE kafka_offsets (topic TEXT NOT NULL, partition INTEGER NOT NULL, "
        "next_offset INTEGER NOT NULL, cluster_id TEXT, updated_at TEXT NOT NULL, PRIMARY KEY (topic, partition))"
    )
    legacy.execute(
        "INSERT INTO kafka_offsets (topic, partition, next_offset, cluster_id, updated_at) VALUES (?, 0, 42, ?, ?)",
        (TOPIC, "cluster-a", utc_now()),
    )
    legacy.commit()

    ensure_schema(legacy)

    assert "topic_id" in {row[1] for row in legacy.execute("PRAGMA table_info(kafka_offsets)")}
    assert stored_offset(legacy, TOPIC, 0) == 42, "the migration must not disturb the checkpoint"
    legacy.close()


def test_database_path_resolves_a_relative_override_against_the_flow(monkeypatch):
    """A relative override must not depend on the shell's cwd."""
    monkeypatch.setenv("BLUESKY_SQLITE_PATH", "data/other.db")
    assert _common.database_path() == _common.PROJECT_ROOT / "data" / "other.db"

    monkeypatch.setenv("BLUESKY_SQLITE_PATH", "/tmp/abs.db")
    assert _common.database_path() == _common.Path("/tmp/abs.db")


def test_connect_db_accepts_the_string_that_db_flag_passes(tmp_path):
    """`--db` is a str; `connect_db` must not call `.parent` on it."""
    connection = connect_db(str(tmp_path / "cli.db"))
    ensure_schema(connection)
    connection.close()

    assert (tmp_path / "cli.db").exists()


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


class FakeSocket:
    """An async frame source that optionally fails like a dropped socket."""

    def __init__(self, frames, error=None):
        self._frames = frames
        self._error = error

    def __aiter__(self):
        return self._frames_async()

    async def _frames_async(self):
        for frame in self._frames:
            yield frame
        if self._error is not None:
            raise self._error


class FakeConnection:
    """The async context manager `websockets.connect` returns."""

    def __init__(self, socket_: FakeSocket):
        self._socket = socket_

    async def __aenter__(self):
        return self._socket

    async def __aexit__(self, *exc):
        return False


def connector(streams: list[FakeSocket], urls: list[str] | None = None):
    """A `connect` that hands out the next scripted socket and records the URL."""

    def connect(url):
        if urls is not None:
            urls.append(url)
        return FakeConnection(streams.pop(0))

    return connect


def sleeper(record: list[float]):
    """An awaitable stand-in for `asyncio.sleep` that records the delay instead."""

    async def sleep(delay: float) -> None:
        record.append(delay)

    return sleep


class Clock:
    """Deterministic monotonic clock: each call returns the next listed value."""

    def __init__(self, values):
        self._values = list(values)
        self._index = 0

    def __call__(self):
        value = self._values[min(self._index, len(self._values) - 1)]
        self._index += 1
        return value


def run_bridge(producer, streams, **kwargs):
    return asyncio.run(pr.bridge(producer, connect=connector(streams), **kwargs))


def test_producer_reconnects_after_a_drop_without_losing_events():
    streams = [
        FakeSocket([wrapped(COMMIT)], error=ConnectionResetError("boom")),
        FakeSocket([wrapped(COMMIT_B)]),
    ]
    producer = FakeProducer()
    slept: list[float] = []

    stats = run_bridge(producer, streams, max_events=2, sleep=sleeper(slept))

    assert stats["events"] == 2
    assert stats["reconnects"] == 1
    assert stats["delivered"] == 2, "both events were acknowledged by the broker"
    assert stats["failed"] == 0
    keys = [record[2] for record in producer.records]
    assert keys == [b"at://did:plc:aaaa/app.bsky.feed.post/3kxyz", b"at://did:plc:bbbb/app.bsky.feed.post/3kxy2"]
    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0]], "a dropped stream backs off from the start of the ladder"


def test_producer_reconnects_when_the_connection_is_recycled():
    """A healthy connection is recycled on a timer, and the replay resumes from the cursor."""
    frames = [wrapped(COMMIT)] * 20
    streams = [FakeSocket(frames), FakeSocket(frames)]
    urls: list[str] = []
    producer = FakeProducer()
    slept: list[float] = []

    stats = asyncio.run(
        pr.bridge(
            producer,
            connect=connector(streams, urls),
            max_events=16,
            reconnect_seconds=14,
            clock=Clock(range(1000)),
            sleep=sleeper(slept),
        )
    )

    assert stats["reconnects"] == 1, "a timer close reconnects without the error ladder"
    assert stats["events"] == 16
    assert len(producer.records) == 16
    assert streams == [], "the run used a second connection rather than ending"
    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0]]
    assert len(urls) == 2
    assert "cursor=" not in urls[0], "a live start carries no cursor"
    assert "cursor=" in urls[1], "the reconnect resumes from the last event, not from the top"


def test_producer_escalates_the_ladder_when_the_stream_ends_cleanly():
    """An upstream that accepts and immediately closes must not be retried once a second.

    The ladder is reset by a *productive* connection, not by connecting: reset at
    connect, and a peer that closes at once is retried forever at the first rung.
    """
    streams = [FakeSocket([]), FakeSocket([]), FakeSocket([wrapped(COMMIT)])]
    slept: list[float] = []

    stats = run_bridge(FakeProducer(), streams, max_events=1, sleep=sleeper(slept))

    assert stats["reconnects"] == 2
    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0], pr.DEFAULT_BACKOFF_SECONDS[1]], "escalating, not flat"


def test_producer_escalates_the_ladder_after_repeated_drops_without_frames():
    streams = [
        FakeSocket([], error=ConnectionResetError("boom")),
        FakeSocket([], error=ConnectionResetError("boom")),
        FakeSocket([wrapped(COMMIT)]),
    ]
    slept: list[float] = []

    run_bridge(FakeProducer(), streams, max_events=1, sleep=sleeper(slept))

    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0], pr.DEFAULT_BACKOFF_SECONDS[1]]


def test_producer_resets_the_ladder_after_a_connection_that_delivered():
    """A healthy peer starts the next failure at the bottom of the ladder again."""
    streams = [
        FakeSocket([wrapped(COMMIT)], error=ConnectionResetError("boom")),
        FakeSocket([wrapped(COMMIT_B)], error=ConnectionResetError("boom")),
        FakeSocket([wrapped(ACCOUNT)]),
    ]
    slept: list[float] = []

    stats = run_bridge(FakeProducer(), streams, max_events=3, sleep=sleeper(slept))

    assert stats["events"] == 3
    assert slept == [pr.DEFAULT_BACKOFF_SECONDS[0]] * 2, "the second drop is not penalised for the first"


def test_producer_never_opens_the_stream_when_already_stopped():
    opened: list[FakeSocket] = []

    def connect(url):
        opened.append(FakeSocket([]))
        return FakeConnection(opened[-1])

    stats = asyncio.run(pr.bridge(FakeProducer(), connect=connect, should_stop=lambda: True))

    assert stats["events"] == 0
    assert opened == [], "a stop flag set before the run must not open a connection"


def test_producer_counts_a_rejected_delivery_as_failed_not_delivered():
    """Enqueued is not delivered: a broker rejection must not be counted as success."""
    stats = run_bridge(
        FakeProducer(delivery_error="UNKNOWN_TOPIC_OR_PART"), [FakeSocket([wrapped(COMMIT)])], max_events=1
    )

    assert stats["enqueued"] == 1
    assert stats["delivered"] == 0
    assert stats["failed"] == 1


class OverflowingProducer(FakeProducer):
    """Raises like librdkafka's full local queue, after recording the message."""

    def produce(self, topic, value=None, key=None, on_delivery=None):
        super().produce(topic, value=value, key=key, on_delivery=on_delivery)
        raise BufferError("Local: Queue full")


def test_producer_flushes_the_queue_when_produce_raises():
    """A crash mid-stream must not abandon events already handed to the client."""
    producer = OverflowingProducer()

    with pytest.raises(BufferError):
        run_bridge(producer, [FakeSocket([wrapped(COMMIT)])])

    assert producer.flushes == [30], "the queue is flushed on the way out"
    assert len(producer.records) == 1


def test_producer_persists_the_delivered_events_own_time():
    """The cursor is inclusive, so storing the event's own `time` replays exactly it.

    `+1` was measured to skip every sibling sharing that microsecond (3.4% of a
    6,000-event window landed in a shared one), so the stored value is the
    delivered event's timestamp, not one past it.
    """
    persisted: list[int] = []

    stats = run_bridge(
        FakeProducer(),
        [FakeSocket([wrapped(COMMIT)])],
        max_events=1,
        on_cursor=persisted.append,
    )

    assert stats["events"] == 1
    assert persisted == [commit_time_us()]


def test_producer_throttles_cursor_persistence_to_the_flush_interval():
    """At ~300 msg/s a write per event would be a SQLite transaction per message."""
    persisted: list[int] = []
    frames = [wrapped({**COMMIT, "rkey": f"r{index}", "seq": COMMIT["seq"] + index}) for index in range(5)]

    run_bridge(FakeProducer(), [FakeSocket(frames)], max_events=5, flush_every=2, on_cursor=persisted.append)

    assert len(persisted) == 3, "two mid-run flushes plus one on the way out"
    assert persisted == sorted(persisted)


def test_producer_never_persists_a_position_the_broker_has_not_confirmed():
    """`produce` only enqueues; persisting there would step over a dropped batch."""

    class SilentProducer(FakeProducer):
        """Never resolves a delivery, like a broker that stopped acknowledging."""

        def produce(self, topic, value=None, key=None, on_delivery=None):
            self.records.append((topic, value, key))

    persisted: list[int] = []

    stats = run_bridge(
        SilentProducer(),
        [FakeSocket([wrapped(COMMIT)])],
        max_events=1,
        flush_every=1,
        on_cursor=persisted.append,
    )

    assert stats["events"] == 1
    assert stats["enqueued"] == 1
    assert stats["delivered"] == 0
    assert persisted == [], "an unacknowledged event cannot move the cursor"


def test_producer_latches_the_cursor_at_the_first_failed_delivery():
    """One rejected message pins the position; later successes must not step over it."""

    class FlakyProducer(FakeProducer):
        """Fails the first delivery, acknowledges the rest."""

        def __init__(self):
            super().__init__()
            self.failures_left = 1

        def produce(self, topic, value=None, key=None, on_delivery=None):
            self.records.append((topic, value, key))
            if on_delivery is not None:
                error = "UNKNOWN_TOPIC_OR_PART" if self.failures_left else None
                self.failures_left = max(0, self.failures_left - 1)
                on_delivery(error, self.records[-1])

    later = {**COMMIT_B, "time": "2026-10-27T15:00:05.000Z"}
    persisted: list[int] = []

    stats = run_bridge(
        FlakyProducer(),
        [FakeSocket([wrapped(COMMIT), wrapped(later)])],
        max_events=2,
        on_cursor=persisted.append,
    )

    assert stats["failed"] == 1
    assert stats["delivered"] == 1
    assert persisted and persisted[-1] == commit_time_us(), "the failed event's position, not the later success"


def test_delivery_counter_reports_nothing_before_the_first_acknowledgement():
    counter = pr.DeliveryCounter()

    assert counter.cursor() is None
    counter.callback(100, None, None)
    assert counter.cursor() == 100
    counter.callback(90, None, None)
    assert counter.cursor() == 100, "only the newest position is a resume point"


def test_delivery_counter_uses_the_earliest_failure():
    counter = pr.DeliveryCounter()

    counter.callback(300, None, None)
    counter.callback(200, "boom", None)
    counter.callback(100, "boom", None)
    assert counter.cursor() == 100, "the earliest hole is the one that matters"
    counter.callback(400, None, None)
    assert counter.cursor() == 100, "a later success cannot step over the hole"


def test_delivery_counter_ignores_a_message_without_a_usable_time():
    counter = pr.DeliveryCounter()

    counter.callback(None, None, None)
    assert counter.cursor() is None
    counter.callback(None, "boom", None)
    assert counter.cursor() is None, "a failure at an unknown position still pins nothing"


def test_producer_advances_the_cursor_over_a_dropped_frame():
    """A frame with no payload cannot be replayed by cursor, so it must not move it."""
    persisted: list[int] = []
    bad = wrapped(NO_SEQ)

    stats = run_bridge(
        FakeProducer(),
        [FakeSocket([bad, wrapped(COMMIT)])],
        max_events=1,
        flush_every=1,
        on_cursor=persisted.append,
    )

    assert stats["dropped"] == 1
    assert persisted == [commit_time_us()] * 2, "the mid-run flush and the final flush both persist"


def test_producer_reports_an_error_frame_instead_of_dropping_it():
    """v2 refuses a bad cursor with an `error` envelope, which carries no payload."""
    persisted: list[int] = []
    refusal = json.dumps({"$type": "error", "message": "Invalid cursor"})

    stats = run_bridge(
        FakeProducer(),
        [FakeSocket([refusal, wrapped(COMMIT)])],
        max_events=1,
        flush_every=1,
        on_cursor=persisted.append,
    )

    assert stats["errors"] == 1, "an upstream refusal is not a corrupt frame"
    assert stats["dropped"] == 0
    assert persisted == [commit_time_us()] * 2


def test_frame_error_reads_the_reason_and_ignores_other_frames():
    assert _common.frame_error(json.dumps({"$type": "error", "message": "Invalid cursor"})) == "Invalid cursor"
    assert _common.frame_error(json.dumps({"$type": "error", "error": "throttled"})) == "throttled"
    assert _common.frame_error(wrapped(COMMIT)) is None
    assert _common.frame_error(b"\xff not json") is None
    assert _common.frame_error(json.dumps(["error"])) is None


def _iso(offset_seconds: float) -> str:
    stamp = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset_seconds)
    return stamp.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _now_us() -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1_000_000)


def test_producer_reports_a_clamped_replay():
    """Past the ~37 h window Jetstream serves its oldest retained event instead."""
    requested = _now_us() - 60 * 3600 * 1_000_000
    served = {**COMMIT, "time": _iso(0)}
    persisted: list[int] = []

    stats = run_bridge(
        FakeProducer(),
        [FakeSocket([wrapped(served)])],
        cursor=requested,
        max_events=1,
        on_cursor=persisted.append,
    )

    assert stats["clamped"] == 1
    assert persisted[-1] == pr.event_time_us(served), "the run continues from the served position"


def test_producer_does_not_report_a_clamp_on_a_real_replay():
    """An event as old as the cursor is a replay, which is the normal path."""
    requested = _now_us() - 60 * 3600 * 1_000_000
    event = {**COMMIT, "time": _iso(-60 * 3600)}

    stats = run_bridge(FakeProducer(), [FakeSocket([wrapped(event)])], cursor=requested, max_events=1)

    assert stats["clamped"] == 0


def test_producer_does_not_report_a_clamp_inside_the_retained_window():
    """A forward jump inside the window is a quiet filter, not a clamp.

    Reporting it used to advise `--live`, which is the one action that skips
    events -- a real hole built to fix a phantom one.
    """
    requested = _now_us() - 3600 * 1_000_000

    stats = run_bridge(
        FakeProducer(),
        [FakeSocket([wrapped({**COMMIT, "time": _iso(0)})])],
        cursor=requested,
        max_events=1,
    )

    assert stats["clamped"] == 0


def test_producer_stops_mid_stream_and_flushes():
    """SIGTERM must stop before the next frame and must not leave the queue undelivered."""
    producer = FakeProducer()
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 2

    run_bridge(producer, [FakeSocket([wrapped(COMMIT), wrapped(COMMIT_B)])], should_stop=should_stop)

    assert producer.flushes == [30]
    assert len(producer.records) == 1, "the stop flag is checked before each frame, so the second is never read"


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
    """The two `confluent_kafka.Consumer` methods `resolve_offsets` calls."""

    def __init__(self, low: int = 0, high: int = 300, cluster_id: str | None = "cluster-a"):
        self.low = low
        self.high = high
        self.cluster_id = cluster_id

    def list_topics(self, topic=None, timeout=None):
        from types import SimpleNamespace

        # A broker reports an absent topic by omitting it from the metadata,
        # which is what `resolve_offsets` turns into a "create it first" exit.
        if topic != TOPIC:
            return SimpleNamespace(cluster_id=self.cluster_id, topics={})
        partition = SimpleNamespace(id=0)
        return SimpleNamespace(
            cluster_id=self.cluster_id,
            topics={topic: SimpleNamespace(partitions={0: partition})},
        )

    def get_watermark_offsets(self, partition, timeout=None):
        return self.low, self.high


def test_resolve_offsets_resumes_from_sqlite(db):
    write_batch(db, [], {(TOPIC, 0): 120}, cluster_id="cluster-a")
    cluster = FakeCluster(low=0, high=300)

    offsets, cluster_id = resolve_offsets(db, cluster, TOPIC)

    assert offsets == {(TOPIC, 0): 120}
    assert cluster_id == "cluster-a", "the cluster id is returned so the checkpoint can record it"


def test_resolve_offsets_refuses_a_retention_gap(db):
    write_batch(db, [], {(TOPIC, 0): 10}, cluster_id="cluster-a")

    with pytest.raises(SystemExit, match="retention"):
        resolve_offsets(db, FakeCluster(low=50, high=300), TOPIC)


def test_resolve_offsets_accepts_a_retention_gap_when_told_to(db):
    write_batch(db, [], {(TOPIC, 0): 10}, cluster_id="cluster-a")

    offsets, _ = resolve_offsets(db, FakeCluster(low=50, high=300), TOPIC, allow_gap=True)

    assert offsets == {(TOPIC, 0): 50}


def test_resolve_offsets_logs_a_first_run_without_a_checkpoint(db, caplog):
    caplog.set_level(logging.INFO, logger="ingestion")

    offsets, _ = resolve_offsets(db, FakeCluster(low=7, high=300), TOPIC)

    assert offsets == {(TOPIC, 0): 7}
    assert "no stored offset" in caplog.text
    assert "outside the log" not in caplog.text


def test_resolve_offsets_resets_when_the_cluster_changed(db):
    """A recreated broker reuses offsets from 0, so the old checkpoint can land mid-log."""
    write_batch(db, [], {(TOPIC, 0): 120}, cluster_id="old-cluster")

    offsets, cluster_id = resolve_offsets(db, FakeCluster(low=0, high=300), TOPIC)

    assert offsets == {(TOPIC, 0): 0}
    assert cluster_id == "cluster-a"


def test_resolve_offsets_refuses_a_missing_topic(db):
    with pytest.raises(SystemExit, match="does not exist"):
        resolve_offsets(db, FakeCluster(), "no.such.topic")


def test_resolve_offsets_resets_when_the_topic_was_recreated(db):
    """A topic recreated on the same broker keeps the cluster id but restarts at 0."""
    write_batch(db, [], {(TOPIC, 0): 120}, cluster_id="cluster-a", topic_id="topic-old")

    offsets, _ = resolve_offsets(db, FakeCluster(low=0, high=300), TOPIC, topic_id="topic-new")

    assert offsets == {(TOPIC, 0): 0}


def test_resolve_offsets_resumes_when_the_topic_id_matches(db):
    write_batch(db, [], {(TOPIC, 0): 120}, cluster_id="cluster-a", topic_id="topic-a")

    offsets, _ = resolve_offsets(db, FakeCluster(low=0, high=300), TOPIC, topic_id="topic-a")

    assert offsets == {(TOPIC, 0): 120}


def test_resolve_offsets_skips_the_topic_check_when_the_id_is_unknown(db):
    """An unreadable id must not reset a checkpoint that is otherwise sound."""
    write_batch(db, [], {(TOPIC, 0): 120}, cluster_id="cluster-a", topic_id="topic-a")

    offsets, _ = resolve_offsets(db, FakeCluster(low=0, high=300), TOPIC, topic_id=None)

    assert offsets == {(TOPIC, 0): 120}


def test_resolve_offsets_logs_a_recreated_topic_as_the_cause(db, caplog):
    caplog.set_level(logging.ERROR, logger="ingestion")
    write_batch(db, [], {(TOPIC, 0): 120}, cluster_id="cluster-a", topic_id="topic-old")

    resolve_offsets(db, FakeCluster(low=0, high=300), TOPIC, topic_id="topic-new")

    assert "topic id topic-old" in caplog.text
    assert "cluster" not in caplog.text.replace("topic id", ""), "the cluster id did not change"


class FakeAdmin:
    """The two `AdminClient` calls `current_identity` makes, and nothing else."""

    def __init__(
        self, cluster_id: str | None = "cluster-a", topic_id: object = "topic-a", error: Exception | None = None
    ):
        self.cluster_id = cluster_id
        self.topic_id = topic_id
        self.error = error

    def list_topics(self, timeout=None):
        return SimpleNamespace(cluster_id=self.cluster_id)

    def describe_topics(self, topics, request_timeout=None):
        error = self.error
        topic_id = self.topic_id

        class Described:
            def result(self):
                if error is not None:
                    raise error
                return SimpleNamespace(topic_id=topic_id)

        assert topics.topic_names == [TOPIC], "the id is asked for by name, not for the whole cluster"
        return {TOPIC: Described()}


def test_current_identity_reads_both_ids(monkeypatch):
    monkeypatch.setattr(_kafka, "AdminClient", lambda config: FakeAdmin())

    assert _kafka.current_identity("localhost:9092", TOPIC) == ("cluster-a", "topic-a")


def test_current_identity_normalises_the_uuid_the_broker_returns(monkeypatch):
    """`TopicDescription.topic_id` is a `cimpl.Uuid`: unprintable to SQLite and never equal to a string."""

    class UuidLike:
        def __str__(self):
            return "LdzjPnBWTxukR30CUHk/Iw"

        def __eq__(self, other):
            return False

    monkeypatch.setattr(_kafka, "AdminClient", lambda config: FakeAdmin(topic_id=UuidLike()))

    assert _kafka.current_identity("localhost:9092", TOPIC) == ("cluster-a", "LdzjPnBWTxukR30CUHk/Iw")


def test_current_identity_reports_an_unreadable_topic_as_none(monkeypatch, caplog):
    caplog.set_level(logging.ERROR, logger="ingestion")
    monkeypatch.setattr(_kafka, "AdminClient", lambda config: FakeAdmin(error=_kafka.KafkaException("UNKNOWN_TOPIC")))

    assert _kafka.current_identity("localhost:9092", TOPIC) == ("cluster-a", None)
    assert "topic id" in caplog.text


def test_current_identity_reports_an_unknown_cluster_as_none(monkeypatch):
    monkeypatch.setattr(_kafka, "AdminClient", lambda config: FakeAdmin(cluster_id=""))

    assert _kafka.current_identity("localhost:9092", TOPIC)[0] is None


# --- Flink job seams --------------------------------------------------------

# `flink_job` is importable without PyFlink: the pyflink imports are inside
# `run()`, so only the SQL and the start-offset planning are testable here.


def test_flink_column_types_the_growing_columns_as_bigint():
    """`offset` is aggregated by `heal_cursor`; `seq` is already 2.65e10, past INT."""
    assert fj.flink_column("offset", "INTEGER") == "BIGINT"
    assert fj.flink_column("seq", "INTEGER") == "BIGINT"
    assert fj.flink_column("partition", "INTEGER") == "INT"
    assert fj.flink_column("record_key", "TEXT NOT NULL") == "STRING"
    assert fj.flink_column("seq", "INTEGER PRIMARY KEY") == "BIGINT", "a rowid alias is still BIGINT here"
    assert fj.flink_column("payload", "TEXT NOT NULL") == "STRING"


def test_flink_column_refuses_an_unmapped_type():
    """Defaulting to INT would truncate a future REAL column on every write, silently."""
    with pytest.raises(ValueError, match="REAL"):
        fj.flink_column("score", "REAL")


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
        for port in range(39300, 39304):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", port))
            sock.listen(1)
            sockets.append(sock)
        assert fj.dashboard_port(39300, span=4) is None
    finally:
        for sock in sockets:
            sock.close()


def test_source_ddl_positions_the_job_at_the_stored_offsets():
    ddl = fj.source_ddl("localhost:9092", {(TOPIC, 0): 620}, bounded=False)

    assert "'scan.startup.mode' = 'specific-offsets'" in ddl
    assert "'scan.startup.specific-offsets' = 'partition:0,offset:620'" in ddl
    assert "'format' = 'raw'" in ddl
    assert "'topic' = 'bluesky.jetstream'" in ddl
    assert "'properties.group.id' = 'bluesky-flink-sink'" in ddl
    assert "bounded" not in ddl, "an unbounded run must not stop at the log end"
    for metadata in ("`partition` INT METADATA FROM 'partition' VIRTUAL", "`offset` BIGINT METADATA FROM 'offset'"):
        assert metadata in ddl


def test_source_ddl_stops_at_the_log_end_only_when_bounded():
    ddl = fj.source_ddl("localhost:9092", {(TOPIC, 0): 0}, bounded=True)

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


def test_insert_sql_maps_the_columns_in_events_order():
    """The SELECT list is positional: a swap writes a did into `collection`."""
    sql = fj.insert_sql()

    assert "?" not in sql, "this is a Flink INSERT ... SELECT; placeholders belong to the dialect"
    select_list = sql.split("FROM jetstream_source")[0]
    projections = {
        "record_key": "JSON_VALUE(payload, '$.record_key')",
        "ingested_at": "CONCAT(DATE_FORMAT(CURRENT_TIMESTAMP",
        "event_dt": "JSON_VALUE(payload, '$.time')",
        "event_kind": "JSON_VALUE(payload, '$.kind')",
        "collection": "JSON_VALUE(payload, '$.collection')",
        "did": "JSON_VALUE(payload, '$.did')",
        "operation": "JSON_VALUE(payload, '$.operation')",
        "rkey": "JSON_VALUE(payload, '$.rkey')",
        "seq": "CAST(JSON_VALUE(payload, '$.seq') AS BIGINT)",
        "topic": f"'{TOPIC}'",
        "partition": "`partition`",
        "offset": "`offset`",
        "payload": "payload\n",
    }
    assert list(projections) == list(EVENT_COLUMNS), "the test's list must track the table"
    positions = [select_list.index(fragment) for fragment in projections.values()]
    assert positions == sorted(positions), "projection order must match EVENT_COLUMNS"


def test_insert_sql_drops_bodies_without_an_identity_or_an_integer_seq():
    """The WHERE clause is `parse_event` in SQL; both rejections must be there.

    The `seq` test must apply to the cast value, not the raw string: `"abc"`
    passes a string null-check and then casts to NULL, and a NULL rowid alias
    makes SQLite invent a key for the row.
    """
    sql = fj.insert_sql()

    assert "JSON_VALUE(payload, '$.record_key') <> ''" in sql
    assert "CAST(JSON_VALUE(payload, '$.seq') AS BIGINT) IS NOT NULL" in sql


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
