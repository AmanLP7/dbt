# pyright: reportMissingImports=false
"""Read `bluesky.jetstream` with Apache Flink and write it into SQLite.

    python -m ingestion.bluesky.flink_job             # stream until stopped
    python -m ingestion.bluesky.flink_job --bounded   # drain the backlog, exit

PyFlink lives in `.flink-venv`, not in the repo's `.venv` (Flink pins
`protobuf<6`, dbt requires `protobuf>=6`, and one environment cannot hold both),
which is why the imports below are unresolvable to pyright under `.venv` - hence
the suppression on line 1.

The job is Flink SQL: the source, the row mapping and the sink are DDL/DML, and
this module supplies what SQL cannot - the jar classpath, the resume offset, and
the path to the database. This job is the flow's only reader of the topic;
`_kafka.py` holds the resume-position logic.

Two things differ from the Wikimedia flow, and both follow from Jetstream v2:

* Events are not flat. `kind` exists only as the `#` suffix of `$type`, and the
  dedupe key is an `at://` URI assembled from `did`, `collection` and `rkey`
  (or from `did`, `kind` and `seq` for account/identity). Neither is expressible
  in Flink SQL, so the producer computes both and writes them into the message
  body as `kind` and `record_key`; this job reads them as ordinary JSON fields.
  A body written by anything other than this producer has no `record_key` and is
  dropped by the filter below - visibly, in the job's row counts.
* Jetstream's own position is `seq`, stored beside the broker's `offset`. `seq`
  is what the producer resumes from; `offset` is what `heal_cursor` aggregates.

Three properties of the sink are deliberate:

* Parallelism is 1. SQLite's WAL mode allows many readers but one writer, and
  two Flink writers would interleave transactions against one file.
* Delivery is at-least-once. `sqlite-jdbc` has no XA support, so Flink cannot
  offer exactly-once here; `INSERT OR IGNORE` on `seq` makes a replay
  free instead. The dialect in `lib/sqlite-dialect/` is what emits that statement.
* The cursor in `main.kafka_offsets` is recovered from `main.events` at startup
  and after a clean exit, because a Flink sink batch cannot include the offset
  update. It therefore lags by at most the rows a crash left uncommitted - the
  safe direction, since the job then replays rather than skips. Recovery counts
  only rows written since the checkpoint was stamped and only within the same
  Kafka cluster, so a recreated broker resets instead of being healed past.
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
from pathlib import Path

from ingestion.bluesky._common import (
    EVENT_COLUMNS,
    PROJECT_ROOT,
    TOPIC,
    acquire_writer_lock,
    bootstrap_servers,
    configure_logging,
    connect_db,
    database_path,
    ensure_schema,
    heal_cursor,
    logger,
    write_batch,
)
from ingestion.bluesky._kafka import current_identity, plan_start_offsets

JARS = (
    "flink-sql-connector-kafka-5.0.0-2.2.jar",
    "flink-connector-jdbc-core-4.1.0-2.2.jar",
    # Built from `lib/sqlite-dialect/` by `lib/build-sqlite-dialect.sh`: Flink
    # ships dialects for Derby, MySQL, PostgreSQL and others, but not SQLite.
    "flink-connector-sqlite-4.1.0-2.2.jar",
    "sqlite-jdbc-3.53.4.0.jar",
)

JAVA_HINT = (
    "PyFlink needs a JDK 17 on the path. Export one before running this module:\n"
    '  export JAVA_HOME="$HOME/.local/jdk/jdk-17.0.21+9"\n'
    '  export PATH="$JAVA_HOME/bin:$PATH"'
)

# The Flink Web Dashboard is served by the local MiniCluster. Wikimedia's job
# already pins 8081, so this flow uses the next block: two flows running at once
# must not fight over a port.
REST_PORT_ENV = "BLUESKY_FLINK_REST_PORT"
DEFAULT_REST_PORT = 8082
# Ten consecutive ports to try when something else already holds the first: a
# second job on a different database should still come up, on its own URL.
REST_PORT_SPAN = 10

# SQLite has no fixed-width integer types, but Flink does: `offset` outgrows
# `INT` at 2^31 messages, and it is the column `heal_cursor` aggregates. `seq`
# is Jetstream's counter -- today ~2.65e10, seven times past `INT`'s limit -- so
# it is BIGINT on the Flink side even though SQLite stores both as INTEGER.
FLINK_TYPES = {
    "TEXT PRIMARY KEY": "STRING",
    "TEXT NOT NULL": "STRING",
    "TEXT": "STRING",
    "INTEGER PRIMARY KEY": "INT",
    "INTEGER": "INT",
}
BIGINT_COLUMNS = {"offset", "seq"}


def require_java() -> None:
    """Fail early, in Python, when the JVM PyFlink needs is not reachable."""
    home = os.environ.get("JAVA_HOME")
    found = shutil.which("java") or (home and Path(home, "bin", "java").is_file())
    if not found:
        raise SystemExit(JAVA_HINT)


def dashboard_port(preferred: int, span: int = REST_PORT_SPAN) -> int | None:
    """First free TCP port at or after `preferred`, or None if the whole range is taken.

    Probing before Flink binds is a check-then-use race, but the loser is a startup
    exception naming the port, and the alternative - a URL that changes every run -
    is worse for the person watching the job.
    """
    for port in range(preferred, preferred + span):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    return None


def jar_uris(lib_dir: Path) -> list[str]:
    """Absolute `file://` URIs for the connector jars, or a message saying why not."""
    missing = [name for name in JARS if not (lib_dir / name).is_file()]
    if missing:
        raise SystemExit(
            f"Missing connector jars under {lib_dir}:\n  "
            + "\n  ".join(missing)
            + "\nRun `lib/fetch-jars.sh` (downloads three) and `lib/build-sqlite-dialect.sh` "
            "(builds the fourth from `lib/sqlite-dialect/`)."
        )
    return [(lib_dir / name).resolve().as_uri() for name in JARS]


def flink_column(name: str, sql_type: str) -> str:
    """Map a `main.events` column definition onto its Flink SQL type.

    An unknown type raises rather than defaulting to `INT`: a new `REAL` column
    would otherwise be declared `INT` and have its values truncated on the way
    into SQLite, at write time, per row, silently.
    """
    if name in BIGINT_COLUMNS:
        return "BIGINT"
    try:
        return FLINK_TYPES[sql_type]
    except KeyError:
        raise ValueError(f"no Flink type for main.events.{name} {sql_type!r}; add it to FLINK_TYPES") from None


def source_ddl(bootstrap: str, start_offsets: dict[tuple[str, int], int], *, bounded: bool) -> str:
    """The Kafka source, positioned at the offsets SQLite says to read next.

    `format = 'raw'` keeps the message body as one string column: the sink stores
    the event verbatim, and the columns beside it are extracted from that string
    by the INSERT. A `json` format would need a declared schema for a payload
    whose shape depends on `$type`.

    The `partition` and `offset` metadata columns are the reason this is Flink
    SQL rather than a PyFlink DataStream job: PyFlink's value-only deserializer
    hands Python the payload and nothing else, and both columns belong in the
    events table (and in how the cursor is recovered).
    """
    specific = ";".join(f"partition:{partition},offset:{offset}" for (_, partition), offset in start_offsets.items())
    bounded_mode = "  'scan.bounded.mode' = 'latest-offset',\n" if bounded else ""
    return f"""
CREATE TEMPORARY TABLE jetstream_source (
  payload STRING,
  `partition` INT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = '{TOPIC}',
  'properties.bootstrap.servers' = '{bootstrap}',
  'properties.group.id' = 'bluesky-flink-sink',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'specific-offsets',
  'scan.startup.specific-offsets' = '{specific}',
{bounded_mode}  'format' = 'raw',
  'raw.charset' = 'UTF-8'
)
"""


def sink_ddl(db_path: Path) -> str:
    """The JDBC sink. `table-name` is `events`; the dialect emits `INSERT OR IGNORE`.

    No primary key is declared: the sink is append-only and the duplicate
    suppression happens in SQLite, not in Flink's upsert machinery.
    """
    columns = ",\n  ".join(f"{_quote(name)} {flink_column(name, sql_type)}" for name, sql_type in EVENT_COLUMNS.items())
    # The JVM resolves a relative `jdbc:sqlite:` path against its own cwd, which
    # is not the shell's and not the flow's, so the URL carries an absolute path.
    url = db_path.resolve().as_posix()
    return f"""
CREATE TEMPORARY TABLE events_sink (
  {columns}
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:sqlite:{url}',
  'table-name' = 'events',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '3'
)
"""


def insert_sql() -> str:
    """Map each raw event onto the thirteen `main.events` columns.

    The filter is `parse_event` in SQL, and drops the same messages: a body with
    no `record_key` (an event that names no record and no account) and a body
    with no integer `seq`. `JSON_VALUE` returns NULL for an absent path, and a
    NULL comparison drops the row, so the `record_key` test needs no extra null
    guard.

    The `seq` test is on the **cast** value, not the string `JSON_VALUE` returns.
    Testing the string accepted `"abc"` and `"123"`, whose `CAST` is NULL, and a
    NULL primary key is not a constraint violation in SQLite: `seq` is a rowid
    alias, so the row would be stored under a fabricated rowid and the next
    genuine event carrying that `seq` would be dropped by `INSERT OR IGNORE`.
    One event silently replacing another is the exact failure this table's key
    exists to prevent, so it is checked where it can still be caught.

    `record_key` and `kind` come from the producer, which writes them into the
    body: the key is an `at://` URI assembled from three nullable fields with a
    different rule per event type, and Flink 2.2 has no right-split of a string,
    so neither is expressible here. The consequence is deliberate and stated:
    this job reads bodies this pipeline produced, not arbitrary Jetstream frames.

    `ingested_at` is rebuilt here rather than taken from the row's Kafka
    timestamp so that it keeps the format the Python reader writes
    (`2026-10-27T12:00:00.000+00:00`): both writers share one column and a
    downstream query must not have to parse two shapes. It is one
    `DATE_FORMAT` call, with the `T` quoted inside the pattern, because
    `CURRENT_TIMESTAMP` is re-evaluated at every reference in a streaming query:
    two calls could straddle midnight and label yesterday's time with today's
    date.
    """
    return f"""
INSERT INTO events_sink
SELECT
  JSON_VALUE(payload, '$.record_key'),
  CONCAT(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss.SSS'), '+00:00'),
  JSON_VALUE(payload, '$.time'),
  JSON_VALUE(payload, '$.kind'),
  JSON_VALUE(payload, '$.collection'),
  JSON_VALUE(payload, '$.did'),
  JSON_VALUE(payload, '$.operation'),
  JSON_VALUE(payload, '$.rkey'),
  CAST(JSON_VALUE(payload, '$.seq') AS BIGINT),
  '{TOPIC}',
  `partition`,
  `offset`,
  payload
FROM jetstream_source
WHERE JSON_VALUE(payload, '$.record_key') <> ''
  AND CAST(JSON_VALUE(payload, '$.seq') AS BIGINT) IS NOT NULL
"""


def _quote(identifier: str) -> str:
    """Backtick a column name; `offset`, `partition` and `operation` are keywords."""
    return f"`{identifier}`"


def run(db_path: Path, lib_dir: Path, *, bounded: bool, allow_gap: bool) -> int:
    """Plan the start offsets, then submit the job and block until it ends."""
    require_java()
    uris = jar_uris(lib_dir)
    acquire_writer_lock(db_path)

    db = connect_db(db_path)
    try:
        ensure_schema(db)
        # The log's identity has to be known *before* the cursor is recovered:
        # `heal_cursor` aggregates rows written against one log, and only the
        # ids can tell that log from a replacement. Heal first and a recreated
        # broker -- or a recreated topic -- would be jumped by the old log's
        # highest offset.
        cluster, topic_id = current_identity(bootstrap_servers(), TOPIC)
        # Recover the cursor from the rows first, so a run interrupted before its
        # final heal still resumes from the last committed event.
        healed = heal_cursor(db, TOPIC, cluster, topic_id)
        if healed:
            logger.info("Recovered %d checkpoint row(s) from main.events.", healed)
        start_offsets, cluster_id = plan_start_offsets(
            db, bootstrap_servers(), TOPIC, allow_gap=allow_gap, topic_id=topic_id
        )
        # Stamp the identity beside the position this run will start from, so a
        # later run can tell a resumed log from a recreated one -- and so the
        # heal at the end of this run only counts the rows written after it.
        write_batch(db, [], start_offsets, cluster_id, topic_id)
    finally:
        db.close()

    from pyflink.common import Configuration
    from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    for uri in uris:
        env.add_jars(uri)

    preferred = int(os.environ.get(REST_PORT_ENV, DEFAULT_REST_PORT))
    rest_port = dashboard_port(preferred)
    configuration = Configuration()
    if rest_port is None:
        logger.warning(
            "Every port in %d-%d is busy; the dashboard will be on a random port this run.",
            preferred,
            preferred + REST_PORT_SPAN - 1,
        )
    else:
        configuration.set_string("rest.port", str(rest_port))
    env.configure(configuration)

    table_env = StreamTableEnvironment.create(env)
    # `ingested_at` is formatted from CURRENT_TIMESTAMP, so the session zone must
    # be UTC or the timestamp would carry the host's offset under a `+00:00` label.
    table_env.get_config().set("table.local-time-zone", "UTC")
    table_env.get_config().set("pipeline.name", "bluesky-jetstream-to-sqlite")

    table_env.execute_sql(source_ddl(bootstrap_servers(), start_offsets, bounded=bounded))
    table_env.execute_sql(sink_ddl(db_path))
    logger.info(
        "Submitting %s job from %s.",
        "bounded" if bounded else "streaming",
        ", ".join(f"{topic}[{partition}]@{offset}" for (topic, partition), offset in start_offsets.items()),
    )
    result = table_env.execute_sql(insert_sql())
    if rest_port is not None:
        # The MiniCluster serves the stock Flink dashboard here: job graph, per-
        # operator counters, backpressure, and the exception that killed a run.
        job_id = result.get_job_client().get_job_id()
        logger.info("Dashboard: http://127.0.0.1:%d/#/job/%s/overview", rest_port, job_id)
    result.wait()

    db = connect_db(db_path)
    try:
        heal_cursor(db, TOPIC, cluster_id, topic_id)
    finally:
        db.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--db", default=None, help="SQLite path (default: data/bluesky.db).")
    parser.add_argument(
        "--bounded",
        action="store_true",
        help="Stop at the topic's end offset instead of streaming; the backlog is drained and the job exits.",
    )
    parser.add_argument(
        "--allow-gap",
        action="store_true",
        help="Start anyway when retention deleted the stored offset, accepting the missing range.",
    )
    parser.add_argument("--lib-dir", default=None, help="Directory holding the connector jars (default: lib/).")
    args = parser.parse_args()

    configure_logging()
    db_path = Path(args.db) if args.db else database_path()
    lib_dir = Path(args.lib_dir) if args.lib_dir else PROJECT_ROOT / "lib"
    logger.info("Sink: %s (main.events, main.kafka_offsets).", db_path)
    try:
        raise SystemExit(run(db_path, lib_dir, bounded=args.bounded, allow_gap=args.allow_gap))
    except KeyboardInterrupt:
        # Flink's job is gone with the JVM; the rows committed by the last sink
        # batch are in SQLite and the cursor is healed on the next start.
        logger.info("Interrupted; the committed batches are in %s.", db_path)


if __name__ == "__main__":
    main()
