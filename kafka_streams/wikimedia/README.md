# wikimedia — Kafka → SQLite flow

Reads Wikimedia's public `recentchange` firehose, puts each event on a local
Kafka topic, and writes the topic into SQLite. Three processes: producer,
broker, and one of two interchangeable readers — Apache Flink or the Python
consumer.

```
stream.wikimedia.org (NDJSON over HTTP)
        │  wikimedia/ingestion/wikimedia/producer.py
        ▼
Kafka topic wikimedia.recentchange  (1 partition, broker from ../docker-compose.yml)
        │
        ├── ingestion/wikimedia/flink_job.py      (Flink SQL)
        └── ingestion/wikimedia/consumer.py       (Python, confluent-kafka)
        ▼
SQLite wikimedia/data/wikimedia.db
  main.events
  main.kafka_offsets
```

Both readers write the same two tables and share one cursor, so either can take
over where the other stopped. The Flink job is the streaming path; the Python
consumer stays as the dependency-light fallback and the reference for the row
mapping.

Shared pieces live one level up, in `kafka_streams/`: the broker compose file,
`requirements.txt`, `pyproject.toml`, and `.env`. This folder holds only the
flow's code, tests, data, and docs.

## Why the producer exists

Wikimedia's Kafka brokers are internal to the Wikimedia Foundation; the only
public door is HTTP. `producer.py` is therefore a bridge, not a client of a
public broker: it reads NDJSON and produces the event JSON unchanged.

## Quickstart

From `kafka_streams/wikimedia/`:

```bash
# One-time: environment (shared deps live one level up)
cp ../.env.example ../.env
pip install -r ../requirements.txt -r ../requirements-dev.txt

# 1. Broker (needs the docker group; use `sg docker -c '…'` if it is stale)
cd .. && docker compose up -d && docker compose ps && cd wikimedia

# 2. Topic (only once per broker volume)
python -m ingestion.wikimedia.producer --create-topic

# 3. Bridge the firehose (runs until stopped)
python -m ingestion.wikimedia.producer
python -m ingestion.wikimedia.producer --max-events 500     # bounded

# 4. Sink (runs until stopped)
python -m ingestion.wikimedia.consumer
python -m ingestion.wikimedia.consumer --max-events 500     # bounded
python -m ingestion.wikimedia.consumer --max-seconds 30
python -m ingestion.wikimedia.consumer --allow-gap          # accept a retention hole

# Offline tests (no broker, no HTTP)
pytest -q
```

The suite covers parsing, canary rejection, undecodable and non-string-id
messages, both sink transactions, the resume clamps (retention gap, changed
cluster), the stop-path flush, the producer's reconnect paths, delivery
accounting and flush-on-crash, and the Flink job's generated SQL and cursor
recovery. It runs under the repo's `.venv`, not `.flink-venv`: `flink_job`
imports PyFlink only inside `run()`, so everything testable is importable
without it.

`--max-events` on the consumer counts messages read, including ones it skips.
Stop with Ctrl-C; both processes flush before exiting.

## The Flink job

`flink_job.py` is Flink SQL: a Kafka source, an INSERT that maps each payload
onto the twelve `main.events` columns, and a JDBC sink. Python supplies only
what SQL cannot — the jar classpath, the resume offset, and the database path.

```bash
# One-time: a separate environment, a JDK 17, and the connector jars.
python -m venv ../.flink-venv && ../.flink-venv/bin/pip install -r ../requirements-flink.txt
./lib/fetch-jars.sh                 # three jars from Maven Central
./lib/build-sqlite-dialect.sh       # the fourth, compiled from lib/sqlite-dialect/

export JAVA_HOME="$HOME/.local/jdk/jdk-17.0.21+9"   # any JDK 17 works
export PATH="$JAVA_HOME/bin:$PATH"

# Stream until stopped, or drain the backlog and exit.
../.flink-venv/bin/python -m ingestion.wikimedia.flink_job
../.flink-venv/bin/python -m ingestion.wikimedia.flink_job --bounded
../.flink-venv/bin/python -m ingestion.wikimedia.flink_job --allow-gap
```

**Why a separate environment.** `apache-flink 2.2.0` depends on `apache-beam`,
which requires `protobuf<6`; dbt requires `protobuf>=6`. Installing PyFlink into
the repo's `.venv` downgrades protobuf and breaks dbt, so it lives in
`.flink-venv` and only this module is run from there. The tests and lint run
under `.venv` because `flink_job` imports PyFlink lazily, inside `run()`.

**Why Flink SQL rather than a PyFlink dataflow.** The events table needs the
Kafka `partition` and `offset` of each message, and PyFlink's value-only
deserializer hands Python the payload and nothing else. Flink SQL exposes both
as metadata columns.

**Why a compiled dialect.** Flink 2.2's `flink-connector-jdbc-core` ships
dialects for Derby, MySQL, PostgreSQL, Oracle, Db2, SQL Server, CrateDB,
OceanBase, and Trino — not SQLite. `lib/sqlite-dialect/` is a `JdbcDialect`
subclass that registers itself through `META-INF/services`, quotes identifiers
with double quotes, and emits `INSERT OR IGNORE` (see below). Nothing loads it
but `flink_job.py`.

The startup cost is a JVM boot: roughly 15 s before the first message, and the
Flink/Beam logging is verbose. A bounded run over the retained backlog finishes
and exits on its own.

### Watching it run

The MiniCluster serves the stock Flink Web Dashboard. The REST port is pinned so
the address is the same on every start:

```
http://127.0.0.1:8081
```

The job page (`/#/job/<id>/overview`) shows the job graph, per-operator record
counts, backpressure, and the exception that killed a run. `flink_job.py` logs
the exact URL, job id included, once the job is submitted — the port is the only
part that varies, and only when 8081 is taken.

- `WIKIMEDIA_FLINK_REST_PORT` overrides the base port; the job tries ten
  consecutive ports from there and logs which one it got.
- The counters are only meaningful while the JVM lives; the durable answer is
  `SELECT COUNT(*), MAX(offset) FROM events` in SQLite (`sqlite3` is not
  installed here — use Python's `sqlite3` module).
- A second job on a different database comes up on the next free port, so one
  dashboard does not hide the other; open the port its log line names.
- `/checkpoints` reads all zeros. The job does not use Flink checkpoints: its
  position is recovered from committed rows by `heal_cursor`.

## Sink schema

Database: `wikimedia/data/wikimedia.db` (WAL, `synchronous=NORMAL`, `busy_timeout=5000`).
The path is resolved from the module, not the shell, so running from any directory
writes to the same file.
Both tables are in the default `main` schema. `<db>.lock` beside the file is the
writer lock (an empty file); deleting it while a reader runs does not release the
lock, which lives on the open descriptor.

| Table | Grain | Key |
|---|---|---|
| `main.events` | one Wikimedia event | `event_id` (`meta.id`) |
| `main.kafka_offsets` | one topic partition | `(topic, partition)` |

`main.kafka_offsets` columns: `topic, partition, next_offset, cluster_id,
updated_at`. `next_offset` is the offset to read next (not the last one read);
`cluster_id` is the Kafka cluster the checkpoint belongs to.

`main.events` columns: `event_id, ingested_at, event_dt, topic, partition,
offset, wiki, type, title, user, bot, payload`.

- `payload` is the event JSON exactly as Wikimedia sent it. Every other column
  is a copy of a field inside it, extracted at write time so plain SQL does not
  need `json_extract`. `topic`/`partition`/`offset` here are this broker's
  position; the same names inside `payload.meta` are Wikimedia's.
- `event_dt` is the event's own time (`meta.dt`); `ingested_at` is when this
  consumer wrote the row. Neither is a primary key.
- Canaries (`meta.domain == "canary"`) and events without `meta.id` are never
  stored. The producer drops them before Kafka; the consumer drops them again,
  so a foreign producer cannot put them in the table.

Inspect it:

```bash
sqlite3 data/wikimedia.db "SELECT wiki, COUNT(*) FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10;"
sqlite3 data/wikimedia.db "SELECT * FROM kafka_offsets;"
```

## Correctness model

- **At-least-once, deduped at the sink.** The producer is idempotent and
  `acks=all`; both readers rely on `INSERT OR IGNORE` on `event_id`, so a
  duplicate delivery writes nothing. The Python consumer gets that from the raw
  statement; the Flink job gets it from `SqliteDialect.getInsertIntoStatement`,
  which is why the dialect jar exists rather than a plain JDBC insert.
- **SQLite is the cursor.** On start both readers read `main.kafka_offsets` and
  position the partition *at that offset*, clamped to the log's watermarks. The
  Python consumer never calls `subscribe()`, so no group rebalance can move
  progress away from the database. (Setting the offset inside `assign()` rather
  than calling `seek()` afterwards is deliberate: `seek()` on a freshly assigned
  partition raises librdkafka `_STATE` "Erroneous state".) Kafka's committed
  offsets are disabled and never used; the Flink job's consumer group exists only
  to satisfy the connector.
- **Insert and offset are one transaction — for the Python consumer.**
  `write_batch` inserts the events and upserts the next offset inside one
  `with db:` block. A crash leaves both or neither; that sink can lag the broker
  but never points past its own rows.
- **The Flink job recovers its cursor from its rows.** A Flink sink batch cannot
  carry the offset update, so the job derives the cursor instead: at startup and
  after a clean exit, `heal_cursor` sets `next_offset` to `MAX(offset) + 1` per
  partition, and only ever forwards. A kill between the last committed batch and
  the next start therefore costs nothing — the rows are in SQLite, and the
  cursor is repaired from them. Measured: killing the job mid-stream after 100
  events left the cursor 100 behind, and the next start healed it with zero gaps
  and zero duplicates. The one residue: a partition whose *last* rows were all
  skipped (canary, no `meta.id`) leaves the cursor short, and the next start
  replays a few messages that `INSERT OR IGNORE` absorbs.
- **First run reads the retained backlog** (`auto.offset.reset=earliest`), then
  resumes exactly where SQLite says.
- **A retention gap stops the reader.** If the stored offset is below the log's
  earliest offset, retention deleted the messages between them. Both readers exit
  with an error naming the missing range instead of silently skipping it;
  `--allow-gap` accepts the hole and logs it at ERROR. Without this a week of
  downtime would look like a clean resume.
- **A checkpoint remembers its broker.** `kafka_offsets.cluster_id` holds the
  Kafka cluster id at write time. A recreated broker reuses offset numbers from
  0, so an old checkpoint can land *inside* the new log and skip its first N
  messages; a changed cluster id resets the partition to the earliest retained
  offset instead.
- **No exactly-once claim.** The producer's HTTP position and Kafka are two
  stores; a restart mid-batch can replay events already produced. On the Flink
  side there is no XA sink for SQLite either, so Flink's own checkpoints cannot
  make the write transactional — the duplicate-suppressing insert is what makes
  a replay harmless.

WAL allows many readers but exactly one writer. Run **one** reader per database
file: never a Flink job and a Python consumer against the same file at once, and
never two Flink jobs, which is why the job pins parallelism to 1. Both entry
points enforce that with an exclusive `flock` on `<db>.lock` and exit with an
error naming the other writer, so a second start fails immediately instead of
failing per batch with "database is locked".

## Configuration

`kafka_streams/.env` (gitignored, shared by every flow there), loaded by both
processes; `python-dotenv` walks up from the module, so the cwd does not matter:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address. Must match `KAFKA_ADVERTISED_LISTENERS`. |
| `WIKIMEDIA_SQLITE_PATH` | `wikimedia/data/wikimedia.db` | Sink file. A relative value resolves against `wikimedia/`, not the shell's cwd. |
| `WIKIMEDIA_FLINK_REST_PORT` | `8081` | Base port for the Flink Web Dashboard. The job takes the first free port from there up to +9 and logs it. |
| `LOG_LEVEL` | `INFO` | Log level. |

The topic name, partition count, flush bounds, and backoff ladder live in
`ingestion/wikimedia/_common.py`. The Flink job reads the same three variables;
its batch bounds, retries, and parallelism are constants in `flink_job.py`.

## Operations

- **Broker memory.** `../docker-compose.yml` sets a 512 MiB heap and a 1 GiB
  container limit. Without the limit an OOM on this box can kill whatever the
  kernel scores highest — Dagster included.
- **Disk.** Kafka retains 7 days (`KAFKA_LOG_RETENTION_HOURS=168`), about 22 GiB
  at ~3.2 GiB/day measured from the live stream.
- **Reconnect.** The producer reconnects every 14 minutes because EventStreams
  closes connections at 15, and backs off on errors. It does not resume by
  position: events produced before a restart are already in Kafka.
- **Batch.** The consumer writes on `--flush-every` rows (default 500) or every
  15 s, whichever comes first. The Flink sink flushes on 500 rows or 1 s, and
  retries a failed batch three times — safe because the insert is idempotent.
- **Flink startup.** A JVM boot costs ~15 s before the first row, against ~1 s
  for the Python consumer. For a one-off drain of a small backlog, the Python
  consumer is the cheaper tool; for a long-running sink, either works.
- **Stopping the Flink job.** Stop the Python process *and its JVM*: `kill` the
  process group (or Ctrl-C in the foreground) rather than one PID. A JVM that
  outlives its parent keeps consuming and writing, and the next start then exits
  on the `.lock` message. The lock is held by the Python process, so it cannot
  detect that leftover — check `pgrep -af ingestion.wikimedia` before blaming the
  sink. Rows the orphan wrote are not lost; `heal_cursor` adopts them on the next
  start.
- **Docker access.** `docker` needs the `docker` group; a shell started before
  the group was granted needs `sg docker -c '…'` or a re-login.
- **Volume reset.** `docker compose down -v` (or recreating the broker's cluster
  metadata) deletes the topic; recreate it with `--create-topic`. The SQLite
  checkpoint then points past the new log's end *or* at a different cluster id,
  and in both cases the consumer restarts from the earliest offset with a
  warning rather than failing. In the first case the old rows stay in
  `main.events`, so `heal_cursor` re-derives their maximum on the next start and
  the new log is re-drained from the start again — harmless duplicates, but a
  full re-read on every restart until the new log's offsets pass the old maximum.
  Removing that topic's rows from `main.events` (or the file) resets cleanly.
- **Volume, not `/tmp`.** The compose file sets `KAFKA_LOG_DIRS=/var/lib/kafka/data`,
  so `docker compose down` without `-v` keeps the topic. Without that variable
  the broker writes to `/tmp` inside the container and every recreation loses it.

## Limits

- One partition is deliberate: total arrival order, and 30 events/s is far below
  any partition limit. Partition by `wiki` only if per-wiki parallelism matters.
- The database is append-only. Nothing expires, and `main.events` grows at the
  stream rate (~2.7 M rows/day at 30 ev/s).
- The Flink job runs in one process with no checkpointing configured: it
  resumes from SQLite on restart, which is why the cursor is healed from the
  rows rather than from a Flink checkpoint directory.
- Not built: schema registry, a checkpoint/savepoint story for the Flink job,
  retention/compaction policy for the SQLite file, Dagster wiring.
