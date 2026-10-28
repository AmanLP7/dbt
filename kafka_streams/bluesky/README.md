# bluesky — Jetstream → Kafka → SQLite flow

Reads Bluesky's public Jetstream firehose over a WebSocket, puts each event on a
local Kafka topic, and writes the topic into SQLite. Three processes: producer,
broker, and one reader — a Flink SQL job.

```
jetstream.us-east.bsky.network (JSON frames over WebSocket, subprotocol xrpc.v1.json)
        │  bluesky/ingestion/bluesky/producer.py
        ▼
Kafka topic bluesky.jetstream  (1 partition, broker from ../docker-compose.yml)
        │
        └── ingestion/bluesky/flink_job.py      (Flink SQL)
        ▼
SQLite bluesky/data/bluesky.db
  main.events
  main.kafka_offsets      (the job owns)
  main.jetstream_cursor   (producer owns)
```

Flink is the only reader: a Python `confluent_kafka` consumer was the flow's
original sink and was deleted on 2026-10-28, so nothing in this tree polls the
topic except the job. `confluent_kafka` is still a dependency, because the
producer uses it to write and the job uses it — metadata only, no messages — to
ask the broker where to resume from.

Shared pieces live one level up, in `kafka_streams/`: the broker compose file,
`requirements.txt`, `pyproject.toml`, and `.env`. This folder holds only the
flow's code, tests, data, and docs.

## Why the producer exists

Bluesky's firehose is not a Kafka broker either — it is a WebSocket served by
Bluesky's relays. `producer.py` is a bridge: it reads frames and produces them to
the local topic, adding two fields (see below) and removing nothing.

Jetstream is the network's public event log. Every relay runs it; the endpoint
used here is Bluesky's hosted instance, and `BLUESKY_JETSTREAM_URL` points the
flow at a self-hosted one.

## Quickstart

From `kafka_streams/bluesky/`:

```bash
# One-time: environment (shared deps live one level up)
cp ../.env.example ../.env
pip install -r ../requirements.txt -r ../requirements-dev.txt

# 1. Broker (needs the docker group; use `sg docker -c '…'` if it is stale)
cd .. && docker compose up -d && docker compose ps && cd bluesky

# 2. Topic (only once per broker volume)
python -m ingestion.bluesky.producer --create-topic

# 3. Bridge the firehose (runs until stopped)
python -m ingestion.bluesky.producer
python -m ingestion.bluesky.producer --max-events 500      # bounded
python -m ingestion.bluesky.producer --collections app.bsky.feed.post
python -m ingestion.bluesky.producer --live                # ignore the cursor, start at the edge

# 4. Sink — Flink, from its own environment and a JDK 17 (see "The Flink job")
export JAVA_HOME="$HOME/.local/jdk/jdk-17.0.21+9" && export PATH="$JAVA_HOME/bin:$PATH"
../.flink-venv/bin/python -m ingestion.bluesky.flink_job             # stream
../.flink-venv/bin/python -m ingestion.bluesky.flink_job --bounded   # drain, exit
../.flink-venv/bin/python -m ingestion.bluesky.flink_job --allow-gap # accept a retention hole

# Offline tests (no broker, no WebSocket)
pytest -q
```

Measured on this machine: the producer delivered 500 events in 10.1 s and 3,000
events in 11.5 s on 2026-10-27 (≈262 events/s, tracking the live rate of 297/s).
The job's own figures are in "The Flink job" below — a JVM start dominates a
small run, so its elapsed time says nothing about throughput.

The suite covers the v2 envelope, the `at://` key and record projection, the
sink transaction, `heal_cursor`, the resume clamps (retention gap, changed
cluster, changed topic, missing topic), the producer's reconnect, timer and clamp
paths, and the Flink job's generated SQL. It runs under the repo's
`.venv`, not `.flink-venv`: `flink_job` imports PyFlink only inside `run()`, so
everything testable is importable without it.

`--max-events` on the producer counts frames read, including ones it drops. Stop
either process with Ctrl-C; both flush before exiting.

## What Jetstream sends

Measured against the live stream on 2026-10-27, not read off documentation:

| Fact | Consequence |
|---|---|
| The upgrade requires `Sec-WebSocket-Protocol: xrpc.v1.json` | `subprotocols=[…]` is mandatory; the server rejects the handshake without it. |
| Every frame is `{"$type": "message", "payload": {…}}` (10,998 of 10,998 sampled) | One unwrap. An `error` frame carries `$type: "error"` and no payload; it is counted in `errors` and its reason logged, not silently dropped. |
| The payload's `$type` ends in `#commit`, `#account` or `#identity` | The event kind is the `#` suffix. **There is no `kind` field** — v1 surfaces one, v2 does not. |
| `commit` carries `collection`, `rkey`, `operation`, `record`, `cid` | A record event: something was created, updated or deleted. |
| `account` and `identity` carry none of those | They name an account, not a record. |
| Every payload carries `seq`, monotonic and global | The event's identity, and a usable upstream resume position. |
| A `time` value is microsecond-resolution and **shared** by siblings | 71 of 5,866 distinct timestamps in a 6,000-event window covered 205 events (3.4%). A `+1` cursor skips every sibling. |
| `?cursor=` takes a `seq` **or** a microsecond timestamp | Both verified to resume. This flow stores the timestamp. |
| `?compress=true` is rejected on the live tail (HTTP 400) | No compression negotiation to tune. |
| An out-of-range `cursor` fails the handshake (HTTP 400) | A corrupt cursor is a startup error, not silent replay. |
| `?collections=` repeats per value; >100 collections or >10,000 DIDs is rejected before the upgrade | A bad filter fails the handshake, not the stream. |

**Measured volume** (30 s simultaneous probe of both protocol versions, same
machine):

| Version | Rate | Bytes/event | Throughput | Per day |
|---|---|---|---|---|
| v1 | 297.2 ev/s | 577 | 167.3 KiB/s | 13.79 GiB |
| v2 | 297.4 ev/s | 687 | 199.4 KiB/s | 16.43 GiB |

v2 is the only version with replay, so it is the only defensible choice for a new
flow; it costs 19% more bytes per event. At 7-day broker retention the topic holds
~115 GiB (`[INFERENCE]`, extrapolated from the measured rate; not captured to
disk here).

## The replay window, and what happens past it

Jetstream retains a rolling window. Measured by looking at the age of the first
event served for a given lookback:

| Requested lookback | First event served | Verdict |
|---|---|---|
| 30 min | 1,802 s | exact |
| 24 h | 86,401 s | exact |
| 25 h | 90,001 s | exact |
| 72 h | 132,742 s | **clamped** |
| 168 h | 132,752 s | clamped to the same point |
| 720 h | — | handshake `TimeoutError` |

So the window is **~36.9 h** (132,742 s), and a cursor older than that is
silently *clamped*: the server serves the oldest retained event instead of
rejecting the cursor. Nothing in the protocol says so.

`producer.py` therefore compares the first replayed event's age with the age the
cursor implied, and reports an ERROR when the difference exceeds
`CLAMP_TOLERANCE_SECONDS` (600 s) — replay of an event *older* than requested is
normal; much *newer* is a clamp. Verified end to end: a cursor rewound three days
produced

```
ERROR Jetstream clamped the resume cursor 1792856926678299: the first event served
is 133939s old but the cursor asked for 259217s, past the ~36 h replay window. …
```

The check only runs for a cursor older than `RETAINED_WINDOW_SECONDS` (36 h).
Inside the window a clamp is physically impossible, and a forward jump there is a
quiet stretch of the filter — an 11-minute gap in `app.bsky.labeler.service` is a
gap in what *matched*, not in what the server kept. The earlier version reported
it and advised `--live`, which is the one action that builds a real hole.

**A clamped run does not catch up.** Replay measured 451 events/s against a live
297/s: a 37 h backlog shrinks by 154 events/s of stream time, which needs ~71 h of
wall time to work through — longer than the window itself. The run fills the
retained window and stays behind. `--live` (or deleting the cursor row) is how an
operator rejoins the live edge; the gap is real and is named, never silent.

## Bridge design

- **Transport.** One WebSocket, one JSON object per frame, already decoded from
  the network's DAG-CBOR — which is why this bridge is ~100 lines and needs no
  decoder.
- **Reconnect.** The producer closes and reopens every 30 minutes on a timer, and
  backs off `(1, 5, 15, 30, 60)` s on an error. Both paths resume from the
  persisted cursor.
- **Ping.** `ping_interval=20`, `ping_timeout=60`. A silent probe died at 39 s on
  the default timeout; a false disconnect costs a replay.
- **Cursor.** The `time` of the newest event librdkafka **acknowledged**, in
  epoch microseconds, persisted in `main.jetstream_cursor` and throttled to every
  500 events. `produce()` only enqueues, so a cursor taken there would move past
  events a broker outage later discards; the delivery callback is the only place
  that knows what landed. The position is latched at the first failed delivery,
  so one rejected message pins the cursor instead of letting later successes step
  over the hole, and it is left untouched until the first acknowledgement so an
  existing value is never overwritten with nothing. The cost is a replay of the
  in-flight window after a crash — bounded by `linger.ms` and absorbed by
  `INSERT OR IGNORE` — which is the direction that costs duplicates, not gaps.

  The stored value is the event's own timestamp, **not one microsecond past it**.
  `time` is microsecond-resolution and siblings share a microsecond: in a
  6,000-event window 5,866 distinct timestamps covered 6,000 events, and 71
  timestamps held ≥2 events (205 events, 3.4%). The server treats `cursor` as
  inclusive, and `+1` was measured to skip all three events sharing the microsecond
  it landed in — a permanent loss on every crash and every 30-minute recycle.
  A `seq` would also resume upstream, and is rejected here on purpose: a `seq` gap
  is normal under any `--collections` filter, so it cannot be used to diagnose a
  clamp. Only a timestamp carries the age a clamp check needs.
- **Projection.** `project()` adds two fields to each payload: `kind` (the `#`
  suffix, so Flink SQL can name it) and `record_key` (the `at://` URI, assembled
  from three nullable fields with a different rule per event type). Neither is
  expressible in Flink 2.2 SQL: there is no right-split of a string and no
  multi-nullable concatenation. Everything upstream sent is preserved.
- **Idempotency.** The producer is idempotent (`enable.idempotence`, `acks=all`)
  and the sink is keyed on `seq`, so at-least-once delivery on both hops costs
  nothing. The queue is sized at 200,000 messages, because the default would
  block `produce()` on a slow broker and back the WebSocket up instead.

## Sink schema

Database: `bluesky/data/bluesky.db` (WAL, `synchronous=NORMAL`, `busy_timeout=5000`).
The path is resolved from the module, not the shell, so running from any directory
writes to the same file. `<db>.lock` beside the file is the writer lock (an empty
file); deleting it while a reader runs does not release the lock, which lives on
the open descriptor.

| Table | Grain | Key | Written by |
|---|---|---|---|
| `main.events` | one Jetstream event | `seq` | producer's reader |
| `main.kafka_offsets` | one topic partition | `(topic, partition)` | the reader |
| `main.jetstream_cursor` | one upstream stream | `stream` | the producer |

`main.events` columns: `record_key, ingested_at, event_dt, event_kind,
collection, did, operation, rkey, seq, topic, partition, offset, payload`.

The warehouse holds a landing schema, `workspace.bluesky`, with one managed Delta
table, `raw_bluesky`: the same 13 columns in the same order, every one `STRING`
(the timestamps keep their ISO-8601 text), clustered by `event_dt`. Nothing writes
to it yet — the flow stops at SQLite, so it is a target waiting for a loader, not
a copy of this data, and it stays empty until one exists.

- **`seq` is the primary key, deliberately not `record_key`.** A record has
  several events — a `create`, then an `update` or a `delete` — and the `at://`
  URI cannot tell them apart. Measured on the first 3,500 live messages: 15 events
  (0.43%) were a second event for a record already stored, and keying the table on
  the URI silently discarded them. Keyed on `seq`, the same 3,500 messages produce
  3,500 rows over 3,485 distinct records. A replay still writes nothing, because a
  replayed event repeats its `seq`.
- **`record_key` is a lookup column**, not the dedupe key:
  `at://<did>/<collection>/<rkey>` for a commit, `at://<did>/<kind>/<seq>` for an
  account or identity event, indexed with `seq` so "everything that happened to
  one record, in order" is one index scan.
- `payload` is the frame exactly as produced, with only the two added fields.
  Every other column is a copy of a field inside it, extracted at write time so
  plain SQL does not need `json_extract`. `topic`/`partition`/`offset` here are
  this broker's position; `seq` is Jetstream's.
- `event_dt` is the event's own time (`payload.time`); `ingested_at` is when this
  flow wrote the row. Neither is a key.
- Events with no `seq`, no `did`, or a payload that is not an object are never
  stored. The producer drops them before Kafka; the job's `WHERE` clause drops
  them again, so a foreign producer cannot put them in the table. `parse_event`
  and that clause are the same two rules written twice, which is why
  `test_every_body_the_producer_sends_satisfies_the_flink_filter` pins the seam:
  the producer must never hand the sink a body it would drop.

Inspect it (`sqlite3` is not installed here — use Python's `sqlite3` module):

```python
import sqlite3

db = sqlite3.connect("data/bluesky.db")
db.execute("SELECT collection, COUNT(*) FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10").fetchall()
db.execute("SELECT * FROM kafka_offsets").fetchall()
db.execute("SELECT * FROM jetstream_cursor").fetchall()
```

## Correctness model

- **At-least-once, deduped at the sink.** The producer is idempotent and
  `acks=all`; the job relies on `INSERT OR IGNORE` on `seq` (from
  `SqliteDialect.getInsertIntoStatement`), so a duplicate delivery writes nothing.
  Measured on 2026-10-28: 400 messages produced across a restart boundary produced
  399 rows, and the one suppressed message was the last event of the previous run
  replayed by the producer's timestamp cursor. Three offsets are absent from the
  13,850-message log for exactly this reason — a replay, not a loss.
- **SQLite is the Kafka cursor.** On start the job reads `main.kafka_offsets` and
  positions the partition *at that offset*, clamped to the log's watermarks, via
  the source's `'scan.startup.specific-offsets'`. Kafka's committed offsets are
  disabled and never used; the job's consumer group exists only to satisfy the
  connector.
- **The upstream cursor is a second, independent position.** `main.jetstream_cursor`
  holds where the *producer* resumes the WebSocket; `main.kafka_offsets` holds
  where a *reader* resumes the topic. They are written by different processes and
  never reconciled, which is what lets the sink be rebuilt from the topic without
  re-reading Bluesky.
- **The job recovers its cursor from its rows.** A Flink sink batch cannot carry
  the offset update, so `heal_cursor` sets `next_offset` to `MAX(offset) + 1`
  per partition at startup and after a clean exit, and only ever forwards.
  (`write_batch` still writes rows and offsets in one transaction — the producer
  uses it for its upstream cursor, and the job stamps its checkpoint through it
  with an empty row list — but the sink's own INSERT never joins that
  transaction.)
  `heal_cursor` counts only rows ingested after the checkpoint was stamped — the
  rows *this* run wrote — and leaves a checkpoint that records a different
  cluster alone. Neither guard is cosmetic: without the timestamp scope a broker
  recreated behind a correct checkpoint at 0 would be jumped by the previous
  log's highest offset on the next start, skipping the whole fresh backlog.
- **First run reads the retained backlog**, then resumes exactly where SQLite
  says: with no stored offset the planner starts at the log's earliest offset.
- **A retention gap stops the job.** If the stored offset is below the log's
  earliest offset, retention deleted the messages between them. The job exits
  with an error naming the missing range instead of silently skipping it;
  `--allow-gap` accepts the hole and logs it at ERROR.
- **A checkpoint remembers its broker and its log.** `kafka_offsets` stores the
  Kafka cluster id *and* the topic id beside the offset. A recreated broker reuses
  offset numbers from 0, so an old checkpoint can land *inside* the new log and
  skip its first N messages; a changed cluster id resets the partition to the
  earliest retained offset instead. Deleting and recreating the *topic* on the same
  broker keeps the cluster id and does the same damage, and only the topic id sees
  it. Measured on the live broker: a checkpoint of 3 against a 5-message log
  resumes at 3 with a matching id and resets to 0 with a different one. An id the
  planner could not obtain (an older row, an unreadable topic) is treated as
  unknown and skips the comparison rather than resetting a sound checkpoint;
  `ensure_schema` adds the column to a sink created before it existed.
- **No exactly-once claim.** The producer's WebSocket position and Kafka are two
  stores; a restart can replay events already produced. On the Flink side there is
  no XA sink for SQLite either — the duplicate-suppressing insert is what makes a
  replay harmless.

WAL allows many readers but exactly one writer. Run **one** job per database
file — never two, which is also why the job pins parallelism to 1. The job takes
an exclusive `flock` on `<db>.lock` and exits with an error naming the other
writer, so a second start fails immediately instead of failing per batch with
"database is locked".

The producer takes the same lock — it already had the file open to read its
cursor — so the job and the producer cannot run against one file at the same
time. **The producer must be stopped while the job starts**, or pointed at
another `--db`. That is the one operational constraint this flow adds over a
Kafka-only bridge.

## The Flink job

`flink_job.py` is the flow's only reader of the topic, and it is Flink SQL: a
Kafka source, an INSERT that maps each payload onto the thirteen `main.events`
columns, and a JDBC sink. Python supplies only what SQL cannot — the jar
classpath, the resume offsets, and the database path. The offset arithmetic
itself lives in `_kafka.py` (`resolve_offsets`), shared with the producer's
cursor handling; nothing polls Kafka from Python.

```bash
# One-time: a separate environment, a JDK 17, and the connector jars.
python -m venv ../.flink-venv && ../.flink-venv/bin/pip install -r ../requirements-flink.txt
./lib/fetch-jars.sh                 # three jars from Maven Central
./lib/build-sqlite-dialect.sh       # the fourth, compiled from lib/sqlite-dialect/

export JAVA_HOME="$HOME/.local/jdk/jdk-17.0.21+9"   # any JDK 17 works
export PATH="$JAVA_HOME/bin:$PATH"

# Drain the backlog and exit — the invocation to verify a change with.
../.flink-venv/bin/python -m ingestion.bluesky.flink_job --bounded

# Stream until stopped.
../.flink-venv/bin/python -m ingestion.bluesky.flink_job

# Accept a retention hole instead of exiting on it.
../.flink-venv/bin/python -m ingestion.bluesky.flink_job --bounded --allow-gap
```

**Why a separate environment.** `apache-flink 2.2.0` depends on `apache-beam`,
which requires `protobuf<6`; dbt requires `protobuf>=6`. Installing PyFlink into
the repo's `.venv` downgrades protobuf and breaks dbt, so it lives in
`.flink-venv` and only this module is run from there. The tests and lint run
under `.venv` because `flink_job` imports PyFlink lazily, inside `run()`.

**Why Flink SQL rather than a PyFlink dataflow.** The events table needs the
Kafka `partition` and `offset` of each message, and PyFlink's value-only
deserializer hands Python the payload and nothing else. Flink SQL exposes both as
metadata columns.

**Why a compiled dialect.** Flink 2.2's `flink-connector-jdbc-core` ships
dialects for Derby, MySQL, PostgreSQL, Oracle, Db2, SQL Server, CrateDB,
OceanBase, and Trino — not SQLite. `lib/sqlite-dialect/` is a `JdbcDialect`
subclass that registers itself through `META-INF/services` and emits
`INSERT OR IGNORE`. The two flows carry identical copies; nothing loads them but
their own `flink_job.py`.

**Why `seq` is BIGINT.** It is ~2.65e10 today, seven times past `INT`. `offset`
is BIGINT for the same reason at a lower threshold, and because `heal_cursor`
aggregates it. `flink_column()` refuses any type it does not map, rather than
defaulting to `INT` and truncating silently.

The startup cost is a JVM boot: roughly 15 s before the first message, and the
Flink/Beam logging is verbose. A bounded run over the retained backlog finishes
and exits on its own. Measured 2026-10-28: 34.06 s wall for a 400-message
backlog, most of it JVM boot and the bounded source's end-of-input detection, so
this number measures startup, not throughput.

The bounded run writes 399 of those 400 messages, and the missing one is the
correct outcome, not a loss: the producer resumed from `jetstream_cursor` at a
timestamp that replayed the last event of the previous run, whose `seq` was
already stored, so `INSERT OR IGNORE` suppressed it. `kafka_offsets` still
advanced to 13,850. Any gap in `main.events` should be checked the same way —
`seq` at the gap's neighbours is consecutive, so nothing was dropped.

A run ends with two `RuntimeError: can't create new thread at interpreter
shutdown` tracebacks from `grpc`'s atexit hook. They are PyFlink teardown noise
and do not indicate a failed job; check the row count and `kafka_offsets`
instead of the exit status.

### Watching it run

The MiniCluster serves the stock Flink Web Dashboard. The REST port is pinned so
the address is the same on every start:

```
http://127.0.0.1:8082
```

The job page (`/#/job/<id>/overview`) shows the job graph, per-operator record
counts, backpressure, and the exception that killed a run. `flink_job.py` logs
the exact URL, job id included, once the job is submitted — the port is the only
part that varies, and only when 8082 is taken.

- `BLUESKY_FLINK_REST_PORT` overrides the base port; the job tries ten
  consecutive ports from there and logs which one it got. 8082, not 8081, so both
  flows' jobs can run at once.
- The counters are only meaningful while the JVM lives; the durable answer is
  `SELECT COUNT(*), MAX(offset) FROM events` in SQLite.
- `/checkpoints` reads all zeros. The job does not use Flink checkpoints: its
  position is recovered from committed rows by `heal_cursor`.

## Configuration

`kafka_streams/.env` (gitignored, shared by every flow there), loaded by both
processes; `python-dotenv` walks up from the module, so the cwd does not matter:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address. Must match `KAFKA_ADVERTISED_LISTENERS`. |
| `BLUESKY_SQLITE_PATH` | `bluesky/data/bluesky.db` | Sink file. A relative value resolves against `bluesky/`, not the shell's cwd. |
| `BLUESKY_JETSTREAM_URL` | the public v2 endpoint | Upstream. Point it at a self-hosted Jetstream to test without the public stream. |
| `BLUESKY_FLINK_REST_PORT` | `8082` | Base port for the Flink Web Dashboard. |
| `LOG_LEVEL` | `INFO` | Log level. |

The topic name, partition count, event kinds, ping cadence, backoff ladder and
clamp tolerance live in `ingestion/bluesky/_common.py`. The Flink job reads the
same variables; its batch bounds, retries, and parallelism are constants in
`flink_job.py`.

## Operations

- **Broker memory.** `../docker-compose.yml` sets a 512 MiB heap and a 1 GiB
  container limit. Without the limit an OOM on this box can kill whatever the
  kernel scores highest — Dagster included.
- **Disk.** Kafka retains 7 days (`KAFKA_LOG_RETENTION_HOURS=168`). At the measured
  199.4 KiB/s that is ~115 GiB, and ~30 GiB more in SQLite at the 3.80× on-disk
  ratio the Wikimedia flow measured — `[INFERENCE]`, nothing here has been run for
  a week. Size the volume before leaving it running.
- **Reconnect.** Every 30 minutes on a timer, and on any socket error after a
  `(1, 5, 15, 30, 60)` s backoff. Both resume from `main.jetstream_cursor`.
- **Batch.** The Flink sink flushes on 500 rows or 1 s, retrying a failed batch
  three times. The producer persists its cursor every 500 events and once on the
  way out — at 300 events/s a write per event would be a SQLite transaction per
  message.
- **Stopping the Flink job.** Stop the Python process *and its JVM*: `kill` the
  process group (or Ctrl-C in the foreground) rather than one PID. Measured
  2026-10-28: `SIGKILL` of the Python parent took the JVM with it within 3 s —
  PyFlink installs a Watchdog that ends the orphan — and 6 rows from its last sink
  batch landed in between. So an orphan is unlikely, and the `.lock` (held by the
  Python process) cannot see one anyway; a start that exits on the lock message
  means another Python writer is alive. `heal_cursor` adopts the rows either way.
- **Docker access.** `docker` needs the `docker` group; a shell started before the
  group was granted needs `sg docker -c '…'` or a re-login.
- **Volume reset.** `docker compose down -v` deletes the topic; recreate it with
  `--create-topic`. The SQLite checkpoint then points past the new log's end *or*
  at a different cluster id, and in both cases the job restarts from the
  earliest offset with a warning rather than failing. The same `-v` does not touch
  `main.jetstream_cursor`: the producer resumes the *upstream* stream from it,
  independently of the broker, which is the point of storing it locally.
- **Volume, not `/tmp`.** The compose file sets `KAFKA_LOG_DIRS=/var/lib/kafka/data`,
  so `docker compose down` without `-v` keeps the topic.

## Limits

- One partition is deliberate: total arrival order, and 300 events/s is far below
  any partition limit. Partition by `collection` only if per-family parallelism
  ever matters — but that breaks total ordering for a given record.
- The database is append-only. Nothing expires, and `main.events` grows at the
  stream rate (~25.7 M rows/day at 297 ev/s).
- Filters are applied upstream by `--collections` and `--kinds`, so a filtered run
  leaves `seq` gaps in `main.events` by design. That is why `seq` cannot be used to
  measure completeness; `MAX(offset) + 1`, the topic's watermarks, and the
  producer's cursor are the three things that can.
- The producer and a reader cannot share the database file at once (see above).
- Not built: schema registry, a checkpoint/savepoint story for the Flink job,
  retention/compaction policy for the SQLite file, per-`--kinds` tables, Dagster
  wiring.
