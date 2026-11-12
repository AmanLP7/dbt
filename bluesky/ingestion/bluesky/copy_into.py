"""Load Bluesky Jetstream events into `workspace.bluesky.raw_bluesky` via `COPY INTO`.

The sibling loader, `load_events`, pushes rows through the SQL parameter channel:
one `INSERT` per 769 rows, each paying a fixed ~2.8 s to bind ten thousand
parameters and commit a Delta version. That is the right shape for a few thousand
incremental rows and the wrong shape for the 1.7 M-row backlog sitting in the
SQLite sink, where the fixed cost alone is ~110 minutes.

This loader instead writes the rows to disk as newline-delimited JSON, uploads
them to a Unity Catalog staging volume, and lands the whole set with a single
`COPY INTO`. Nothing crosses the parameter channel, so neither term of the INSERT
cost model applies.

    python -m ingestion.bluesky.copy_into --dry-run --assume-empty
    python -m ingestion.bluesky.copy_into --limit 100000
    python -m ingestion.bluesky.copy_into

The guarantee is **at-least-once**, and for a different reason than the INSERT
path. `COPY INTO` keeps a load history keyed by file path, so re-running the same
statement skips files it already committed — that protects a retry within a run.
It does not dedupe row content: the same rows under a new filename load again.
Across runs the resume point is still the target's own `MAX(offset)` per
partition, read fresh at the start, so a run that committed before losing its
acknowledgement is not repeated. Any model over this table must key on
`(partition, offset)` or `seq` regardless.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from databricks.sql.exc import ServerOperationError

from ingestion.bluesky import _common

logger = logging.getLogger(__name__)


@dataclass
class CopyResult:
    """What a run did, for the summary and for the tests."""

    rows: int = 0
    files: int = 0
    file_bytes: int = 0
    statement_seconds: float = 0.0
    export_seconds: float = 0.0
    upload_seconds: float = 0.0
    affected_rows: int = 0
    watermarks: dict[int, int] = field(default_factory=dict)
    run_dir: str | None = None

    @property
    def warehouse_seconds(self) -> float:
        """Statement time plus the scale-to-zero countdown the idling warehouse bills."""
        return self.statement_seconds + _common.AUTO_STOP_SECONDS

    @property
    def dbu(self) -> float:
        return self.warehouse_seconds / 3600 * _common.WAREHOUSE_DBU_PER_HOUR

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
        "--volume",
        default=None,
        help=f"staging volume, catalog.schema.volume (default: ${_common.VOLUME_ENV} or {_common.volume_fqn()})",
    )
    parser.add_argument(
        "--max-file-bytes",
        type=int,
        default=_common.NDJSON_FILE_BYTES,
        help=f"target size of each NDJSON file (default: {_common.NDJSON_FILE_BYTES})",
    )
    parser.add_argument("--limit", type=int, default=None, help="load at most this many rows, then stop cleanly")
    parser.add_argument("--dry-run", action="store_true", help="report the pending rows and their cost, then exit")
    parser.add_argument(
        "--assume-empty",
        action="store_true",
        help="with --dry-run, plan against an empty target instead of reading its watermark",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="where to write NDJSON before uploading (default: a fresh directory under the system temp dir)",
    )
    parser.add_argument(
        "--keep-files",
        action="store_true",
        help="leave the uploaded files in the volume after a successful load, for inspection",
    )
    args = parser.parse_args(argv)
    if args.assume_empty and not args.dry_run:
        # `COPY INTO` would append the whole source a second time under fresh
        # filenames, which its per-path load history does not catch.
        parser.error("--assume-empty only applies to --dry-run")
    if args.max_file_bytes < 1:
        parser.error("--max-file-bytes must be positive")
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
    rows: int,
    bound_bytes: int,
    export_bytes: int,
    watermarks: dict[int, int],
    bounds: dict[int, tuple[int, int]],
) -> _common.CopyEstimate:
    """Log the pending workload, priced both ways, so the choice is visible.

    The two estimates are not alternatives to be averaged: they are the same
    backlog through two mechanisms. Printing both is what turns "which loader
    should I run" from a guess into arithmetic.
    """
    estimate = _common.estimate_copy_cost(rows, export_bytes)
    insert = _common.estimate_cost(rows, bound_bytes)
    logger.info(
        "Plan: %d rows pending (%.1f MB of bound parameters, %.1f MB as NDJSON, resume point %s, source range %s).",
        rows,
        bound_bytes / 1e6,
        export_bytes / 1e6,
        dict(sorted(watermarks.items())) or "none",
        {partition: f"{low}-{high}" for partition, (low, high) in bounds.items()},
    )
    if rows == 0:
        logger.info("Nothing pending: the target already holds every source row.")
        return estimate
    logger.info(
        "COPY INTO: 1 statement, %d file%s of ~%.0f MB -> ~%.1f min of warehouse time, ~%.2f DBU, ~$%.2f.",
        estimate.files,
        "" if estimate.files == 1 else "s",
        estimate.file_bytes / estimate.files / 1e6,
        estimate.minutes,
        estimate.dbu,
        estimate.usd,
    )
    logger.info(
        "  for comparison, INSERT batches: %d statements -> ~%.1f min, ~%.2f DBU, ~$%.2f (%.0fx).",
        insert.statements,
        insert.minutes,
        insert.dbu,
        insert.usd,
        insert.usd / estimate.usd if estimate.usd else 0.0,
    )
    logger.info(
        "  client-side, outside the warehouse bill: ~%.1f min to export, ~%.1f min to upload.",
        estimate.export_seconds / 60,
        estimate.upload_seconds / 60,
    )
    return estimate


def clear_staging(volume_files: _common.VolumeFiles, root: str) -> int:
    """Delete leftover `load_*` directories from earlier runs.

    A run that dies between upload and commit leaves its files behind, and nothing
    will ever reference them again. Removing them at the start of the next run
    keeps the volume from growing by a backlog every time something goes wrong;
    a live run's directory is not at risk because only one loader writes here at a
    time and each run clears before it writes.
    """
    removed = 0
    for stale in volume_files.list_dirs(root, prefix=_common.STAGING_PREFIX):
        removed += volume_files.remove_dir(stale)
        logger.info("Removed stale staging directory %s (%d files).", stale, removed)
    return removed


def copy_into_events(
    *,
    db_path: Path | None = None,
    table: str | None = None,
    volume: str | None = None,
    max_file_bytes: int = _common.NDJSON_FILE_BYTES,
    limit: int | None = None,
    dry_run: bool = False,
    assume_empty: bool = False,
    work_dir: Path | None = None,
    keep_files: bool = False,
    volume_files: _common.VolumeFiles | None = None,
) -> CopyResult:
    """Export the unloaded rows of `db_path` to a volume and land them with one `COPY INTO`."""
    if assume_empty and not dry_run:
        raise ValueError("assume_empty only applies to a dry run")
    if max_file_bytes < 1:
        raise ValueError("max_file_bytes must be positive")
    _common.configure_logging()
    env = _common.require_env()
    path = db_path or _common.sqlite_path()
    con = _common.connect_sqlite(path)
    bounds = _common.source_offset_bounds(con)
    if not bounds:
        logger.warning("No %s rows in %s; nothing to load.", _common.TOPIC, path)

    result = CopyResult()
    connection = None
    local_dir: Path | None = None
    try:
        fqn = table or _common.raw_table_fqn()

        if dry_run:
            if assume_empty:
                watermarks: dict[int, int] = {}
            else:
                connection, cursor = _common.connect(env)
                watermarks = target_watermarks_or_empty(cursor, fqn)
            pending_rows, pending_bytes = _common.pending_stats(con, watermarks)
            planned = pending_rows if limit is None else min(limit, pending_rows)
            per_row = pending_bytes / pending_rows if pending_rows else 0.0
            overhead = _common.ndjson_overhead_bytes(con, watermarks) if planned else 0.0
            log_plan(planned, int(per_row * planned), int((per_row + overhead) * planned), watermarks, bounds)
            result.watermarks = dict(watermarks)
            return result

        connection, cursor = _common.connect(env)
        fqn = _common.ensure_raw_table(cursor, table)
        volume_fqn = _common.ensure_volume(cursor, volume)
        root = _common.volume_root(volume_fqn)
        watermarks = _common.target_watermarks(cursor, fqn)

        pending_rows, pending_bytes = _common.pending_stats(con, watermarks)
        _common.log_gaps(con, watermarks)
        export_rows = pending_rows if limit is None else min(limit, pending_rows)
        per_row = pending_bytes / pending_rows if pending_rows else 0.0
        overhead = _common.ndjson_overhead_bytes(con, watermarks) if export_rows else 0.0
        log_plan(export_rows, int(per_row * export_rows), int((per_row + overhead) * export_rows), watermarks, bounds)
        result.watermarks = dict(watermarks)
        if export_rows == 0:
            return result

        volume_files = volume_files or _common.VolumeFiles()
        clear_staging(volume_files, root)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = f"{root}/{_common.STAGING_PREFIX}{stamp}"
        result.run_dir = run_dir
        local_dir = work_dir or Path(tempfile.mkdtemp(prefix="bluesky-export-"))

        # Export and upload one file at a time, so local disk holds at most one
        # file and progress appears as it happens rather than after twenty minutes
        # of silent I/O.
        exports = _common.export_ndjson(con, watermarks, local_dir, file_bytes=max_file_bytes, max_rows=export_rows)
        while True:
            export_started = time.perf_counter()
            try:
                exported = next(exports)
            except StopIteration:
                break
            result.export_seconds += time.perf_counter() - export_started

            upload_started = time.perf_counter()
            volume_files.upload(f"{run_dir}/{exported.path.name}", exported.path)
            upload_seconds = time.perf_counter() - upload_started
            result.upload_seconds += upload_seconds
            exported.path.unlink()
            result.files += 1
            result.rows += exported.rows
            result.file_bytes += exported.size
            logger.info(
                "File %d: %d rows, %.1f MB, uploaded in %.1fs.",
                result.files,
                exported.rows,
                exported.size / 1e6,
                upload_seconds,
            )

        statement = _common.copy_into_statement(fqn, run_dir)
        logger.info("Running %s", statement.replace("\n", " "))
        statement_started = time.perf_counter()
        cursor.execute(statement + ";")
        copy_result = cursor.fetchall()
        result.statement_seconds = time.perf_counter() - statement_started
        logger.debug("COPY INTO returned %s", copy_result)
        result.affected_rows = _affected_rows(copy_result)

        # The truth about what landed, from the target rather than from the
        # statement's own report: one cheap read that also becomes the resume
        # point for the next run.
        result.watermarks = _common.target_watermarks(cursor, fqn)

        if not keep_files:
            volume_files.remove_dir(run_dir)
            result.run_dir = None
    finally:
        con.close()
        if connection is not None:
            connection.close()
        if local_dir is not None and work_dir is None:
            shutil.rmtree(local_dir, ignore_errors=True)

    logger.info(
        "Loaded %d rows from %d file%s in one statement (%.1f min of warehouse time, %.2f DBU, ~$%.2f). "
        "Resume point: %s.",
        result.rows,
        result.files,
        "" if result.files == 1 else "s",
        result.warehouse_seconds / 60,
        result.dbu,
        result.usd,
        dict(sorted(result.watermarks.items())),
    )
    return result


def _affected_rows(rows: object) -> int:
    """Pull `num_affected_rows` out of a COPY INTO result, if the connector returned it.

    The column is not guaranteed — some warehouse versions answer with a
    per-file row and some with nothing — so the value is reported when present
    and left at zero when not, rather than treated as truth.
    """
    if not rows:
        return 0
    for row in rows:  # type: ignore[union-attr]
        try:
            return int(row[0]) if row else 0
        except (TypeError, ValueError, IndexError):
            continue
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        copy_into_events(
            db_path=args.db,
            table=args.table,
            volume=args.volume,
            max_file_bytes=args.max_file_bytes,
            limit=args.limit,
            dry_run=args.dry_run,
            assume_empty=args.assume_empty,
            work_dir=args.work_dir,
            keep_files=args.keep_files,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted; a `COPY INTO` either committed or did not, and the next run reads the target.")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
