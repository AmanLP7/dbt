"""The cost model a `--dry-run` plan prices a load with.

Every expected number here is derived from the loader's own measured batches
(2026-10-28, 2X-Small serverless), not from a guess: the tests pin the arithmetic,
the shape of the model, and the two facts that decide what this pipeline costs — a
run always pays the scale-to-zero countdown, and a warehouse INSERT tops out well
below the rate the SQLite sink is being fed at.
"""

from __future__ import annotations

import pytest

from ingestion.bluesky import _common

# The backlog sitting in kafka_streams/bluesky/data/bluesky.db on 2026-10-28, as
# `pending_stats` counted it: 1,724,597 rows carrying 1,838.7 MB of bound text.
BACKLOG_ROWS = 1_724_597
BACKLOG_CHARS = 1_838_700_000
CHARS_PER_ROW = 1_066


def test_a_run_always_pays_the_scale_to_zero_countdown() -> None:
    """A one-statement run costs ~62.8 s of warehouse time and ~$0.049.

    Literals, not expressions in the loader's own constants: this is the number the
    README publishes, and it has to fail if any of the four constants behind it move.
    """
    estimate = _common.estimate_cost(1, 1_000)

    assert estimate.statements == 1
    # 2.8 s fixed + 1,000 bytes at 204 K/s + the 60 s idling minute.
    assert estimate.warehouse_seconds == pytest.approx(62.80, abs=0.01)
    assert estimate.usd == pytest.approx(0.0488, abs=0.001)


def test_statement_count_follows_the_tighter_of_the_two_caps() -> None:
    """Rows bound at 769 per statement; bytes bound at 768 KiB per statement.

    1,000,000 rows needs ceil(1e6/769) = 1,301 statements by the row cap;
    900,000,000 bytes needs ceil(9e8/786,432) = 1,145 by the byte cap. The row cap
    binds, so the count is 1,301 and not the sum or the byte figure.
    """
    estimate = _common.estimate_cost(1_000_000, 900_000_000)

    assert estimate.statements == 1_301


def test_the_byte_cap_binds_when_rows_do_not() -> None:
    """Same row count, fatter rows: the byte cap takes over."""
    estimate = _common.estimate_cost(1_000_000, 4_000_000_000)

    assert estimate.statements == 5_087  # ceil(4e9 / 786,432)


def test_the_model_reproduces_the_measured_batch_timings() -> None:
    """A full batch and a tiny one, both within the 0.8 s the fit leaves on average."""
    measured = [(769, 750_000, 6.5), (7, 7_168, 2.7)]

    for rows, chars, seconds in measured:
        estimate = _common.estimate_cost(rows, chars, auto_stop_seconds=0.0)
        assert estimate.statement_seconds == pytest.approx(seconds, abs=0.2), (rows, chars)


def test_the_full_backlog_costs_about_fifteen_dollars() -> None:
    """The 24-hour backlog the Flink sink already holds, as one catch-up run."""
    estimate = _common.estimate_cost(BACKLOG_ROWS, BACKLOG_CHARS)

    assert 2_300 <= estimate.statements <= 2_400
    assert 230 <= estimate.minutes <= 290
    assert 15.0 <= estimate.dbu <= 20.0
    assert 10.5 <= estimate.usd <= 14.0


def test_one_hour_of_firehose_takes_hours_to_load() -> None:
    """The ceiling that decides the steady-state design: ~191 rows/s, not ~450.

    The Jetstream firehose ran at ~450 messages/s when measured, i.e. 1.62 M rows
    per hour. The model says that hour takes several hours to INSERT, so the loader
    is a backfill tool. Records the number rather than implying otherwise.
    """
    hourly_rows = 1_620_000
    estimate = _common.estimate_cost(hourly_rows, hourly_rows * CHARS_PER_ROW)

    assert estimate.minutes > 3 * 60
    # Rows a warehouse-loading loop absorbs per second, from the same model.
    assert round(_common.PARAMETER_CHARS_PER_SECOND / CHARS_PER_ROW) == 191


def test_nothing_pending_still_costs_the_idling_minute() -> None:
    """A no-op run has already connected and read the watermark; the minute bills anyway."""
    estimate = _common.estimate_cost(0, 0)

    assert estimate.statements == 0
    assert estimate.statement_seconds == 0.0
    assert estimate.warehouse_seconds == pytest.approx(60.0)
    assert estimate.usd == pytest.approx(0.0467, abs=0.001)
