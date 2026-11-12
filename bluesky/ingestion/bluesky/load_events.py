"""Load Bluesky Jetstream events out of SQLite and into `workspace.bluesky.raw_bluesky`.

The SQLite database is the sink of the Flink job in `kafka_streams/bluesky/`; the
target is its text mirror in Unity Catalog. The load is append-only and resumable
from the target's own `MAX(offset)` per partition, so an interrupted run loses
nothing: the next run re-reads whatever did not land.

The guarantee is **at-least-once, not exactly-once**. The watermark is advanced in
memory only after an `INSERT` returns, so a statement that committed on the server
but lost its acknowledgement (Ctrl-C, a dropped socket, a cancelled statement)
leaves the target ahead of nothing: the next run re-sends that batch and the target
holds its rows twice. Up to 769 duplicate `(partition, offset)` rows, and
`target_watermarks` cannot see them, because it only asks for `MAX(offset)`. Any
model built on this table must therefore key on `(partition, offset)` — or `seq`,
which is Jetstream's own per-event counter — rather than assuming one row per event.

Warehouse cost is uptime, so the loader is built around issuing as few statements
as DBSQL allows: one connection for the whole run, one `INSERT` per batch, batches
held open until either DBSQL cap binds, and a loop that stops the moment the
backlog is drained rather than polling. `--dry-run` prices a load before it runs;
every run prints what it actually spent.

    python -m ingestion.bluesky.load_events --dry-run --assume-empty
    python -m ingestion.bluesky.load_events --limit 10000
    python -m ingestion.bluesky.load_events
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from databricks.sql.exc import ServerOperationError

from ingestion.bluesky import _common

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """What a run did, for the summary and for the tests."""

    rows: int = 0
    statements: int = 0
    chars: int = 0
    seconds: float = 0.0
    watermarks: dict[int, int] = field(default_factory=dict)
    stopped_early: str | None = None

    @property
    def dbu(self) -> float:
        """Warehouse DBUs this run bought, including the scale-to-zero countdown."""
        return (self.seconds + _common.AUTO_STOP_SECONDS) / 3600 * _common.WAREHOUSE_DBU_PER_HOUR

    @property
    def usd(self) -> float:
        return self.dbu * _common.USD_PER_DBU


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"SQLite event database (default: ${_common.SQLITE_PATH_ENV} or {_common.DEFAULT_SQLITE_PATH})",
    )
    parser.add_argument(
        "--table",
        default=None,
        help=f"target table (default: {_common.CATALOG}.{_common.SCHEMA}.{_common.RAW_TABLE})",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=_common.INSERT_CHUNK_ROWS,
        help=f"row cap per INSERT, at most {_common.MAX_ROWS_PER_STATEMENT} (default: {_common.INSERT_CHUNK_ROWS})",
    )
    parser.add_argument("--limit", type=int, default=None, help="load at most this many rows, then stop cleanly")
    parser.add_argument("--max-seconds", type=float, default=None, help="stop after this much wall time")
    parser.add_argument("--dry-run", action="store_true", help="report the pending rows and their cost, then exit")
    parser.add_argument(
        "--assume-empty",
        action="store_true",
        help="with --dry-run, plan against an empty target instead of reading its watermark",
    )
    args = parser.parse_args(argv)
    if args.assume_empty and not args.dry_run:
        # A real run with this flag would append every source row to the target a
        # second time, and nothing downstream dedupes yet.
        parser.error("--assume-empty only applies to --dry-run")
    return args


def target_watermarks_or_empty(cursor: _common.SqlExecutor, table: str) -> dict[int, int]:
    """The target's resume points; a table that does not exist yet is empty, not an error."""
    try:
        return _common.target_watermarks(cursor, table)
    except ServerOperationError as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            logger.info("Target %s does not exist yet; starting from the beginning of the source.", table)
            return {}
        raise


def log_plan(
    rows: int, chars: int, watermarks: dict[int, int], bounds: dict[int, tuple[int, int]]
) -> _common.CostEstimate:
    """Log the pending workload and what the measured cost model expects it to buy."""
    estimate = _common.estimate_cost(rows, chars)
    logger.info(
        "Plan: %d rows pending (%.1f MB of bound parameters, resume point %s, source range %s) "
        "-> %d statements, ~%.1f min of warehouse time, ~%.2f DBU, ~$%.2f.",
        rows,
        chars / 1e6,
        dict(sorted(watermarks.items())) or "none",
        {partition: f"{low}-{high}" for partition, (low, high) in bounds.items()},
        estimate.statements,
        estimate.minutes,
        estimate.dbu,
        estimate.usd,
    )
    return estimate


def load_events(
    *,
    db_path: Path | None = None,
    table: str | None = None,
    batch_rows: int = _common.INSERT_CHUNK_ROWS,
    limit: int | None = None,
    max_seconds: float | None = None,
    dry_run: bool = False,
    assume_empty: bool = False,
) -> LoadResult:
    """Land the unloaded rows of `db_path` into the raw table; returns what happened."""
    if assume_empty and not dry_run:
        raise ValueError("assume_empty only applies to a dry run")
    _common.configure_logging()
    env = _common.require_env()
    path = db_path or _common.sqlite_path()
    con = _common.connect_sqlite(path)
    bounds = _common.source_offset_bounds(con)
    if not bounds:
        logger.warning("No %s rows in %s; nothing to load.", _common.TOPIC, path)

    result = LoadResult()
    started = time.perf_counter()
    connection = None
    try:
        fqn = table or _common.raw_table_fqn()

        if dry_run:
            # A dry run creates nothing, so an absent target simply plans as empty.
            if assume_empty:
                watermarks: dict[int, int] = {}
            else:
                connection, cursor = _common.connect(env)
                watermarks = target_watermarks_or_empty(cursor, fqn)
            pending_rows, pending_chars = _common.pending_stats(con, watermarks)
            log_plan(pending_rows, pending_chars, watermarks, bounds)
            result.watermarks = dict(watermarks)
            return result

        # A real run connects and creates the table first, so the resume query
        # cannot fail on a target that has never been loaded.
        connection, cursor = _common.connect(env)
        fqn = _common.ensure_raw_table(cursor, table)
        watermarks = _common.target_watermarks(cursor, fqn)

        pending_rows, pending_chars = _common.pending_stats(con, watermarks)
        _common.log_gaps(con, watermarks)
        log_plan(pending_rows, pending_chars, watermarks, bounds)
        result.watermarks = dict(watermarks)
        if pending_rows == 0:
            logger.info("Nothing pending: every source row is already in %s.", fqn)
            return result

        started = time.perf_counter()
        # Bound the scan to what the plan just measured. The source is a live sink
        # and page reads resume past the previous page, so an unbounded drain would
        # keep finding freshly-sunk rows and never finish against a firehose faster
        # than the warehouse (measured ~191 rows/s against ~450/s). A run therefore
        # loads exactly the backlog it printed, in a finite time, and the next run
        # picks up whatever arrived meanwhile.
        budget = pending_rows if limit is None else min(limit, pending_rows)
        for batch in _common.iter_batches(con, watermarks, batch_rows=batch_rows, max_rows=budget):
            batch_started = time.perf_counter()
            batch_chars = sum(_common.row_chars(row) for row in batch)
            _common.insert_batch(cursor, fqn, batch)
            result.rows += len(batch)
            result.statements += 1
            result.chars += batch_chars
            _common.advance_watermarks(result.watermarks, batch)
            logger.info(
                "Batch %d: %d rows, %d KB, %.1fs, resume point %s.",
                result.statements,
                len(batch),
                batch_chars // 1024,
                time.perf_counter() - batch_started,
                dict(sorted(result.watermarks.items())),
            )
            elapsed = time.perf_counter() - started
            if max_seconds is not None and elapsed >= max_seconds:
                result.stopped_early = f"wall-time budget of {max_seconds:.0f}s reached"
                logger.info("Stopping: %s. The committed batches are in %s.", result.stopped_early, fqn)
                break
            if limit is not None and result.rows >= limit:
                result.stopped_early = f"row limit of {limit} reached"
                logger.info("Stopping: %s.", result.stopped_early)
                break
    finally:
        con.close()
        if connection is not None:
            connection.close()
        result.seconds = time.perf_counter() - started

    logger.info(
        "Loaded %d rows in %d statements (%.1f min, %.2f DBU, ~$%.2f). Resume point: %s.",
        result.rows,
        result.statements,
        result.seconds / 60,
        result.dbu,
        result.usd,
        dict(sorted(result.watermarks.items())),
    )
    if result.stopped_early is None:
        logger.info("Backlog drained.")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_events(
            db_path=args.db,
            table=args.table,
            batch_rows=args.batch_rows,
            limit=args.limit,
            max_seconds=args.max_seconds,
            dry_run=args.dry_run,
            assume_empty=args.assume_empty,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted; every committed batch is in the target and the next run resumes from it.")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
