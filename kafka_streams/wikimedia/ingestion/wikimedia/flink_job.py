# pyright: reportMissingImports=false
"""Read `wikimedia.recentchange` with Apache Flink and write it into SQLite.

    python -m ingestion.wikimedia.flink_job             # stream until stopped
    python -m ingestion.wikimedia.flink_job --bounded   # drain the backlog, exit

PyFlink lives in `.flink-venv`, not in the repo's `.venv` (Flink pins
`protobuf<6`, dbt requires `protobuf>=6`, and one environment cannot hold both),
which is why the imports below are unresolvable to pyright under `.venv` - hence
the suppression on line 1.

The job is Flink SQL: the source, the row mapping and the sink are DDL/DML, and
this module supplies what SQL cannot - the jar classpath, the resume offset, and
the path to the database. `consumer.py` remains the Python reader for the same
topic; `_kafka.py` holds the positioning both share.

Three properties of the sink are deliberate:

* Parallelism is 1. SQLite's WAL mode allows many readers but one writer, and
  two Flink writers would interleave transactions against one file.
* Delivery is at-least-once. `sqlite-jdbc` has no XA support, so Flink cannot
  offer exactly-once here; `INSERT OR IGNORE` on `event_id` makes a replay free
  instead. The dialect in `lib/sqlite-dialect/` is what emits that statement.
* The cursor in `main.kafka_offsets` is recovered from `main.events` at startup
  and after a clean exit, because a Flink sink batch cannot include the offset
  update. It therefore lags by at most the rows a crash left uncommitted - the
  safe direction, since the job then replays rather than skips.
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
from pathlib import Path

from ingestion.wikimedia._common import (
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
from ingestion.wikimedia._kafka import plan_start_offsets

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

# The Flink Web Dashboard is served by the local MiniCluster. Left alone, its REST
# port is chosen at random on every start, so a bookmarked URL dies each restart;
# pinning it is what makes `http://127.0.0.1:8081` a stable address.
REST_PORT_ENV = "WIKIMEDIA_FLINK_REST_PORT"
DEFAULT_REST_PORT = 8081
# Ten consecutive ports to try when something else already holds the first: a
# second job on a different database should still come up, on its own URL.
REST_PORT_SPAN = 10

# SQLite has no fixed-width integer types, but Flink does: an offset outgrows
# `INT` at 2^31 messages, and `events.offset` is the column `heal_cursor`
# aggregates, so it is typed BIGINT on the Flink side.
FLINK_TYPES = {"TEXT PRIMARY KEY": "STRING", "TEXT NOT NULL": "STRING", "TEXT": "STRING", "INTEGER": "INT"}
BIGINT_COLUMNS = {"offset"}


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
    by the INSERT. A `json` format would need a declared schema and would drop
    fields Wikimedia adds upstream.

    The `partition` and `offset` metadata columns are the reason this is Flink
    SQL rather than a PyFlink DataStream job: PyFlink's value-only deserializer
    hands Python the payload and nothing else, and both columns belong in the
    events table (and in how the cursor is recovered).
    """
    specific = ";".join(f"partition:{partition},offset:{offset}" for (_, partition), offset in start_offsets.items())
    bounded_mode = "  'scan.bounded.mode' = 'latest-offset',\n" if bounded else ""
    return f"""
CREATE TEMPORARY TABLE recentchange_source (
  payload STRING,
  `partition` INT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = '{TOPIC}',
  'properties.bootstrap.servers' = '{bootstrap}',
  'properties.group.id' = 'wikimedia-flink-sink',
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
    """Map each raw event onto the twelve `main.events` columns.

    The filter is `parse_event` in SQL, and drops the same messages: a body with
    no `meta.id` (the dedupe key) and Wikimedia's canary traffic, which is
    health-check noise rather than wiki activity. `JSON_VALUE` returns NULL for
    an absent path or a JSON null, and a NULL comparison drops the row, so the
    `id` test needs no extra null guard. It does stringify a non-string id where
    `parse_event` would reject one; the value is only ever a primary key, so the
    difference costs nothing and Flink 2.2 has no `JSON_TYPE` to test it with.

    `ingested_at` is rebuilt here rather than taken from the row's Kafka
    timestamp so that it keeps the format the Python reader writes
    (`2026-10-27T12:00:00.000+00:00`): both writers share one column and a
    downstream query must not have to parse two shapes. It is one
    `DATE_FORMAT` call, with the `T` quoted inside the pattern, because
    `CURRENT_TIMESTAMP` is re-evaluated at every reference in a streaming query:
    two calls could straddle midnight and label yesterday's time with today's
    date.

    `bot` is `LOWER(JSON_VALUE(...)) = 'true'` where `event_row` uses Python's
    `bool()`. They agree on a JSON boolean, which is what Wikimedia sends, and on
    the strings `"true"` and `"false"`. A foreign producer sending `1` or
    `"False"` would get 0 here and 1 from `event_row` — both diverge in the same
    direction, and neither is a value Wikimedia emits. Same class of divergence
    as the `meta.id` note above.
    """
    return f"""
INSERT INTO events_sink
SELECT
  JSON_VALUE(payload, '$.meta.id'),
  CONCAT(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss.SSS'), '+00:00'),
  JSON_VALUE(payload, '$.meta.dt'),
  '{TOPIC}',
  `partition`,
  `offset`,
  JSON_VALUE(payload, '$.wiki'),
  JSON_VALUE(payload, '$.type'),
  JSON_VALUE(payload, '$.title'),
  JSON_VALUE(payload, '$.user'),
  CASE WHEN LOWER(JSON_VALUE(payload, '$.bot')) = 'true' THEN 1 ELSE 0 END,
  payload
FROM recentchange_source
WHERE JSON_VALUE(payload, '$.meta.id') <> ''
  AND COALESCE(JSON_VALUE(payload, '$.meta.domain'), '') <> 'canary'
"""


def _quote(identifier: str) -> str:
    """Backtick a column name; `offset`, `partition` and `type` are keywords."""
    return f"`{identifier}`"


def run(db_path: Path, lib_dir: Path, *, bounded: bool, allow_gap: bool) -> int:
    """Plan the start offsets, then submit the job and block until it ends."""
    require_java()
    uris = jar_uris(lib_dir)
    acquire_writer_lock(db_path)

    db = connect_db(db_path)
    try:
        ensure_schema(db)
        # Recover the cursor from the rows first, so a run interrupted before its
        # final heal still resumes from the last committed event.
        healed = heal_cursor(db, TOPIC)
        if healed:
            logger.info("Recovered %d checkpoint row(s) from main.events.", healed)
        start_offsets, cluster_id = plan_start_offsets(db, bootstrap_servers(), TOPIC, allow_gap=allow_gap)
        # Stamp the cluster id beside the position this run will start from, so a
        # later run can tell a resumed broker from a recreated one.
        write_batch(db, [], start_offsets, cluster_id)
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
    table_env.get_config().set("pipeline.name", "wikimedia-recentchange-to-sqlite")

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
        heal_cursor(db, TOPIC)
    finally:
        db.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--db", default=None, help="SQLite path (default: data/wikimedia.db).")
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
