"""Land the Reddit extractors' SQLite dump into `workspace.topic_raw` via `COPY INTO`.

The three extractors (`extract_reddit.py`, `extract_reddit_comments.py`,
`extract_reddit_users.py`) fetch from Reddit and dump every fetched row into one
local SQLite database (`REDDIT_SQLITE_PATH`, default
`reddit_pipeline/data/reddit.db`). This loader exports each of the three raw
tables to newline-delimited JSON, uploads the files to a Unity Catalog staging
volume, and lands them with one `COPY INTO` per table.

    python -m ingestion.reddit.copy_into --dry-run
    python -m ingestion.reddit.copy_into

The target is append-only: the dbt staging models dedupe by id, so re-landing a
run never needs an upsert. `COPY INTO` keeps a load history keyed by file path,
so a retried run skips files it already committed — it does not dedupe row
content.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ingestion.reddit import _common

logger = logging.getLogger(__name__)


@dataclass
class CopyResult:
    """What a run did: rows landed per raw table, plus the file and directory accounting."""

    rows: dict[str, int] = field(default_factory=dict)
    files: int = 0
    run_dir: str | None = None


def copy_into_raw(
    *,
    db_path: Path | None = None,
    volume: str | None = None,
    tables: Sequence[str] | None = None,
    dry_run: bool = False,
    work_dir: Path | None = None,
    keep_files: bool = False,
    volume_files: _common.VolumeFiles | None = None,
) -> CopyResult:
    """Export the extractors' SQLite dump to a volume and land each table with one `COPY INTO`.

    `tables` selects which raw tables to land; `None` (the default) means every
    table in `_common.RAW_TABLE_COLUMNS`. `dry_run=True` counts rows per selected
    SQLite table and returns without opening a Databricks connection. Otherwise
    the selected raw tables are ensured, their rows are exported to NDJSON and
    uploaded, and each non-empty table gets one `COPY INTO`. Leftover `load_*`
    staging directories from earlier runs are cleared first, and the run's own
    directory is removed afterwards unless `keep_files` is set.
    """
    if tables is None:
        tables = tuple(_common.RAW_TABLE_COLUMNS)
    else:
        tables = tuple(tables)
        unknown = [name for name in tables if name not in _common.RAW_TABLE_COLUMNS]
        if unknown:
            raise ValueError(
                f"Unknown raw table(s): {', '.join(unknown)}. Expected one of {', '.join(_common.RAW_TABLE_COLUMNS)}."
            )
    _common.configure_logging()
    path = db_path or _common.sqlite_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"No SQLite database at {path}. Run the extractors first "
            "(python -m ingestion.reddit.extract_reddit, "
            "extract_reddit_comments, and extract_reddit_users), or set "
            f"{_common.REDDIT_SQLITE_PATH_ENV}."
        )
    con = _common.connect_sqlite(path)

    result = CopyResult()
    connection = None
    local_dir: Path | None = None
    try:
        if dry_run:
            for table in tables:
                count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                result.rows[table] = int(count)
                logger.info("SQLite table %s holds %d rows.", table, result.rows[table])
            return result

        connection, cursor = _common.connect()
        vfqn = _common.ensure_volume(cursor, volume)
        root = _common.volume_root(vfqn)
        volume_files = volume_files or _common.VolumeFiles()
        for stale in volume_files.list_dirs(root, prefix=_common.STAGING_PREFIX):
            volume_files.remove_dir(stale)
            logger.info("Removed stale staging directory %s.", stale)

        run_dir = f"{root}/{_common.STAGING_PREFIX}{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        result.run_dir = run_dir
        local_dir = work_dir or Path(tempfile.mkdtemp(prefix="reddit-export-"))

        for table in tables:
            columns = _common.RAW_TABLE_COLUMNS[table]
            fqn = _common.full_table(table)
            _common.ensure_table(cursor, fqn, columns)
            _common.ensure_columns(cursor, fqn, columns)
            out = local_dir / table
            rows = 0
            for exported in _common.export_ndjson(con, table, columns, out):
                volume_files.upload(f"{run_dir}/{table}/{exported.path.name}", exported.path)
                exported.path.unlink()
                result.files += 1
                rows += exported.rows
                logger.info(
                    "Uploaded %s/%s (%d rows, %.1f MB).",
                    table,
                    exported.path.name,
                    exported.rows,
                    exported.size / 1e6,
                )
            result.rows[table] = rows
            if rows == 0:
                logger.info("No rows for %s; skipping COPY INTO.", fqn)
                continue
            statement = _common.copy_into_statement(fqn, f"{run_dir}/{table}", columns)
            logger.info("Running %s", statement.replace("\n", " "))
            cursor.execute(statement + ";")

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
        "COPY INTO landed %s.",
        ", ".join(f"{table}: {rows} rows" for table, rows in result.rows.items()),
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"SQLite database (default: ${_common.REDDIT_SQLITE_PATH_ENV} or {_common.DEFAULT_SQLITE_PATH})",
    )
    parser.add_argument(
        "--volume",
        default=None,
        help=f"staging volume, catalog.schema.volume (default: ${_common.REDDIT_VOLUME_ENV} or {_common.volume_fqn()})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report the row count per SQLite table, then exit without touching Databricks",
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
    copy_into_raw(
        db_path=args.db,
        volume=args.volume,
        dry_run=args.dry_run,
        work_dir=args.work_dir,
        keep_files=args.keep_files,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
