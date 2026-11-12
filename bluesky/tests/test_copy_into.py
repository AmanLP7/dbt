"""The `COPY INTO` path: what it writes, what it refuses, and what it costs.

These tests are the offline contract for a loader whose failure modes are not
exceptions but silently wrong data — a JSON key that does not match a column
lands `NULL`, and a stale staging directory re-loads rows that are already in the
target. Both are checked here, because neither is visible in a green build.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.bluesky import _common, copy_into
from tests.fakes import FakeVolumeFiles, event_row

# Four rows with a payload that will not survive an ASCII-escaped round trip, so a
# regression to `ensure_ascii=True` fails loudly instead of passing on plain text.
UNICODE_PAYLOAD = '{"text":"héllo 🌍 \\"quoted\\""}'


def _rows() -> list[tuple[object, ...]]:
    return [event_row(offset, payload=UNICODE_PAYLOAD) for offset in range(4)]


# --- NDJSON framing -----------------------------------------------------------


def test_a_line_names_every_column_so_the_load_matches_by_name() -> None:
    """`COPY INTO ... FILEFORMAT = JSON` binds fields by key, and a missing key is NULL.

    A positional encoder would look correct and load empty columns, so the keys are
    asserted against `RAW_COLUMNS` rather than against a copy of the list.
    """
    record = json.loads(_common.ndjson_line(_rows()[0]))

    assert list(record) == list(_common.RAW_COLUMNS)
    assert record["payload"] == UNICODE_PAYLOAD
    assert record["partition"] == 0


def test_non_ascii_stays_utf8_instead_of_escapes() -> None:
    """The emoji cost 4 bytes, not the 12 an escaped `\\ud83c\\udf0d` would take.

    The firehose is emoji-dense, so escaping would inflate the upload by a large
    fraction of its size for nothing.
    """
    line = _common.ndjson_line(_rows()[0])

    assert "héllo 🌍".encode() in line
    assert b"\\u" not in line


def test_a_line_is_one_row_and_ends_in_a_newline() -> None:
    """NDJSON is newline-delimited: no separator but `\\n`, and exactly one at the end."""
    line = _common.ndjson_line(_rows()[0])

    assert line.endswith(b"\n")
    assert line.count(b"\n") == 1


def test_framing_overhead_is_positive_and_counted_per_row() -> None:
    """JSON repeats thirteen key names on every row, so it is always bigger than the binding."""
    import sqlite3

    con = sqlite3.connect(":memory:")
    con.execute(
        "CREATE TABLE events (record_key TEXT, ingested_at TEXT, event_dt TEXT, event_kind TEXT, "
        "collection TEXT, did TEXT, operation TEXT, rkey TEXT, seq INTEGER, topic TEXT, "
        "partition INTEGER, offset INTEGER, payload TEXT)"
    )
    con.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", _rows())

    overhead = _common.ndjson_overhead_bytes(con, {})

    assert overhead > 0
    # Thirteen keys with their colons and quotes is a few hundred bytes; the check
    # is that it is the same order as the payload, not a rounding artefact.
    assert 100 < overhead < 1_000


# --- file splitting -----------------------------------------------------------


def test_files_are_split_at_the_byte_target(source_db, tmp_path: Path) -> None:
    """Every file but the last is within one row of the target, and the rest of the rows follow."""
    import sqlite3

    db = source_db([event_row(offset, payload="y" * 100) for offset in range(50)])
    con = sqlite3.connect(db)
    out = tmp_path / "out"

    files = list(_common.export_ndjson(con, {}, out, file_bytes=2_000))
    con.close()

    assert len(files) > 1
    assert sum(f.rows for f in files) == 50
    assert all(f.size <= 2_000 + 1_000 for f in files[:-1])
    assert all(f.path.exists() for f in files)
    # Names sort in write order, which is what makes a partial run's files obvious.
    assert [f.path.name for f in files] == sorted(f.path.name for f in files)


def test_a_limit_stops_at_exactly_that_many_rows(source_db, tmp_path: Path) -> None:
    """`--limit` cannot overshoot into a whole file's worth of extra rows."""
    import sqlite3

    db = source_db([event_row(offset, payload="z" * 100) for offset in range(50)])
    con = sqlite3.connect(db)

    files = list(_common.export_ndjson(con, {}, tmp_path / "out", file_bytes=100_000, max_rows=7))
    con.close()

    assert sum(f.rows for f in files) == 7


# --- the statement ------------------------------------------------------------


def test_copy_into_names_the_table_the_directory_and_the_format() -> None:
    """The statement is assembled from three facts and must keep `force = 'false'`."""
    statement = _common.copy_into_statement(
        "workspace.bluesky.raw_bluesky", "/Volumes/workspace/bluesky/staging/load_x"
    )

    assert "COPY INTO workspace.bluesky.raw_bluesky" in statement
    assert "FROM (SELECT" in statement
    assert "FROM '/Volumes/workspace/bluesky/staging/load_x'" in statement
    assert "FILEFORMAT = JSON" in statement
    # The dedupe guarantee is this one option: without it a retry duplicates the run.
    assert "'force' = 'false'" in statement


def test_the_projection_casts_only_the_columns_that_need_it() -> None:
    """JSON reads every number as `BIGINT`; `partition` is `INT` and would not merge.

    The three counters get an explicit cast, the ten text columns are passed
    through, and the selection covers every column of the target exactly once — a
    missing one loads `NULL`, a wrongly named one fails the whole statement.
    """
    selection = _common.copy_selection()

    assert "CAST(`partition` AS INT) AS `partition`" in selection
    assert "CAST(`offset` AS BIGINT) AS `offset`" in selection
    assert "CAST(`payload`" not in selection
    # One expression per column, in declaration order: a dropped column loads NULL.
    assert selection.split(", ") == [
        f"`{name}`" if kind == "STRING" else f"CAST(`{name}` AS {kind}) AS `{name}`"
        for name, kind in _common.RAW_COLUMNS.items()
    ]


def test_volume_paths_derive_from_the_table_unless_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_common.VOLUME_ENV, raising=False)

    assert _common.volume_fqn() == "workspace.bluesky.staging"
    assert _common.volume_root() == "/Volumes/workspace/bluesky/staging"

    monkeypatch.setenv(_common.VOLUME_ENV, "other.schema.landing")
    assert _common.volume_root() == "/Volumes/other/schema/landing"


# --- refusals -----------------------------------------------------------------


def test_assume_empty_is_refused_outside_a_dry_run() -> None:
    """`--assume-empty` on a real run would append the whole source a second time.

    `COPY INTO` would not catch it: the new run writes new filenames, and its load
    history is keyed by path.
    """
    with pytest.raises(SystemExit):
        copy_into.parse_args(["--assume-empty"])

    with pytest.raises(ValueError, match="dry run"):
        copy_into.copy_into_events(assume_empty=True, dry_run=False)


def test_dry_run_writes_nothing(monkeypatch: pytest.MonkeyPatch, source_db) -> None:
    """A plan touches the warehouse with reads only: no volume, no upload, no statement."""
    from tests.fakes import FakeWarehouse

    db = source_db(_rows())
    fake = FakeWarehouse()
    monkeypatch.setattr(_common, "connect", fake.connect)
    monkeypatch.setattr(copy_into, "_common", _common)
    volumes = FakeVolumeFiles()

    result = copy_into.copy_into_events(
        db_path=db,
        dry_run=True,
        assume_empty=True,
        volume_files=volumes,  # type: ignore[arg-type]
    )

    assert result.rows == 0
    assert volumes.uploads == []
    assert fake.connects == 0
    assert not any("COPY INTO" in statement for statement, _ in fake.cursor_obj.statements)


# --- staging hygiene ----------------------------------------------------------


def test_clear_staging_removes_only_this_loader_directories() -> None:
    """Anything not named `load_*` belongs to someone else and is left alone."""
    files = FakeVolumeFiles(
        dirs=["/Volumes/workspace/bluesky/staging/load_20261028T170000Z", "/Volumes/workspace/bluesky/staging/other"],
        files=[
            "/Volumes/workspace/bluesky/staging/load_20261028T170000Z/part-00000.json",
            "/Volumes/workspace/bluesky/staging/other/keepme",
        ],
    )

    removed = copy_into.clear_staging(files, "/Volumes/workspace/bluesky/staging")  # type: ignore[arg-type]

    assert removed == 1
    # The API hands back `load_…/` with a trailing slash, and the loader must match
    # it — taking the last segment of the raw path yields "" and matches nothing.
    assert files.removed == ["/Volumes/workspace/bluesky/staging/load_20261028T170000Z/"]
    # The unrelated directory and its file survive.
    assert files.listing("/Volumes/workspace/bluesky/staging/other") == [
        "/Volumes/workspace/bluesky/staging/other/keepme"
    ]


# --- cost model ---------------------------------------------------------------


def test_a_copy_into_run_is_one_statement_plus_the_countdown() -> None:
    """Literals pin the published numbers: the 2.26 GB backlog is ~$0.063, not ~$12.

    The sizes are the ones the export actually produced on 2026-10-28 — 1,724,597
    rows and 2.257 GB of NDJSON — because the file count and the byte term both
    depend on them, and quoting the bound-parameter size here would under-price
    the export by the JSON framing.

    The byte term is the source read rate, not the 204 K/s parameter rate: the
    warehouse reads its own object storage, which is the entire reason this path
    exists.
    """
    estimate = _common.estimate_copy_cost(1_724_597, 2_257_000_000)

    assert estimate.statements == 1
    # 3.30 s fixed + 2.257e9 bytes at 126 MB/s + the 60 s countdown.
    assert estimate.warehouse_seconds == pytest.approx(81.2, abs=0.1)
    assert estimate.usd == pytest.approx(0.0632, abs=0.001)
    assert estimate.files == 12  # ceil(2.257e9 / 200 MB)


def test_the_client_side_terms_are_wall_time_not_money() -> None:
    """Export and upload cost no DBU, but they are 11 of the 13 minutes an operator waits.

    Modelled at the measured 69 MB/s export and 7.7 MB/s upload: the Files API is
    the bottleneck by an order of magnitude, and no warehouse time accrues while
    it runs.
    """
    estimate = _common.estimate_copy_cost(1_724_597, 2_257_000_000)

    assert estimate.export_seconds == pytest.approx(32.7, abs=0.1)
    assert estimate.upload_seconds == pytest.approx(293.1, abs=0.1)
    assert estimate.wall_seconds == pytest.approx(81.2 + 32.7 + 293.1, abs=0.2)
    # None of that client time is billed.
    assert estimate.dbu < 0.1


def test_an_empty_backlog_still_pays_the_countdown_and_nothing_else() -> None:
    """No rows means no statement and no transfer; the minute of idling is the floor."""
    estimate = _common.estimate_copy_cost(0, 0)

    assert estimate.statements == 0
    assert estimate.warehouse_seconds == pytest.approx(60.0)
    assert estimate.usd == pytest.approx(0.0467, abs=0.001)
    assert estimate.export_seconds == 0.0
    assert estimate.wall_seconds == pytest.approx(60.0)
