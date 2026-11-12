# bluesky

ELT over the Bluesky Jetstream firehose. `ingestion/` copies the local SQLite sink
(`kafka_streams/bluesky/data/bluesky.db`) into Unity Catalog; dbt reads the landed
table from there.

The local sink is the source of truth for the raw bytes. `kafka_streams/bluesky/`
owns the WebSocket producer and the Flink job that writes `main.events`; this project
never talks to Jetstream and never writes the SQLite file — it opens it read-only.

## Shape

|Path|Contents|
|---|---|
|`ingestion/bluesky/load_events.py`|CLI: land the rows the target has not seen, one batched `INSERT` per statement.|
|`ingestion/bluesky/copy_into.py`|CLI: the same job through a volume — export NDJSON, upload, one `COPY INTO`. ~160× cheaper on a backlog, slower on a few thousand rows.|
|`ingestion/bluesky/_common.py`|Constants, env validation, the Databricks client, the target table DDL, the SQLite reader, batching, and the cost model.|
|`models/staging/bluesky/src_bluesky.yml`|Source `bluesky_raw.raw_bluesky` — the declaration only; no model yet.|
|`tests/`|Offline pytest: batching, resume, gap detection, cost arithmetic. No warehouse, no SQLite fixture file needed.|
|`profiles/profiles.yml`|target `dev`, catalog `workspace`, schema `bluesky`.|
|`workspace.bluesky.staging`|Unity Catalog volume: the upload target for `COPY INTO`. Transient — cleared at the start of every run.|

## Quick start

```bash
# from the checkout root, one environment for all projects
pip install -r bluesky/requirements.txt -r bluesky/requirements-dev.txt

cd bluesky
cp .env.example .env        # DATABRICKS_HOSTNAME / DATABRICKS_HTTP_PATH

# the INSERT path: right for a few thousand incremental rows
python -m ingestion.bluesky.load_events --dry-run --assume-empty   # price the backlog
python -m ingestion.bluesky.load_events --limit 3000               # prove the path
python -m ingestion.bluesky.load_events                            # drain it

# the COPY INTO path: right for a backlog
python -m ingestion.bluesky.copy_into --dry-run --assume-empty     # price it both ways
python -m ingestion.bluesky.copy_into --limit 100000               # prove the path
python -m ingestion.bluesky.copy_into                              # drain it
```

`--dry-run` reads SQLite and, unless `--assume-empty` is passed, the target; it writes
nothing. `--assume-empty` is refused without `--dry-run` — a real run would append the
whole source a second time. Every run, dry or not, prints what the work costs before
doing it and what it actually spent after.

Flags for `load_events`: `--db PATH` (else `BLUESKY_SQLITE_PATH`, else the
kafka_streams path), `--table CATALOG.SCHEMA.TABLE` (else
`BLUESKY_CATALOG`/`BLUESKY_SCHEMA`, else `workspace.bluesky.raw_bluesky`),
`--batch-rows N` (capped at 769), `--limit N`, `--max-seconds S`, `--assume-empty`.

`copy_into` takes the same `--db`/`--table`/`--limit`/`--dry-run`/`--assume-empty`,
plus `--volume CATALOG.SCHEMA.VOLUME` (else `BLUESKY_VOLUME`, else
`workspace.bluesky.staging`), `--max-file-bytes N` (default 200 MB),
`--work-dir PATH` and `--keep-files` (leave the NDJSON in the volume to inspect).

dbt:

```bash
DBT_PROFILES_DIR=profiles dbt parse     # no warehouse needed
DBT_PROFILES_DIR=profiles dbt build     # writes; needs a token
```

dbt does not read `.env`. Export `DATABRICKS_TOKEN` (or
`$(databricks auth token --profile DEFAULT)`) before a manual `dbt build`.

## The load

**The resume point lives in the target.** Every run asks
`SELECT partition, MAX(offset) FROM raw_bluesky GROUP BY partition` and reads past it.
A local checkpoint can disagree with what actually landed; the target cannot. A crash
costs at most one batch of replay, and a re-run of a completed load is a no-op that
still pays the countdown (see costs).

**Delivery is at-least-once, not exactly-once.** The watermark advances in memory only
after an `INSERT` returns, so a statement the server committed whose acknowledgement
was lost — Ctrl-C, a dropped socket, a cancelled statement — is re-sent by the next
run and lands twice. At most one batch, 769 rows. `MAX(offset)` cannot see the
duplicate, and nothing here dedupes: any model over this table must key on
`(partition, offset)` or `seq`, not assume one row per event. A `MERGE` per batch
would fix it and roughly double the statement count, which is the whole cost.

**Each read page is fetched before anything is inserted.** The source is a live WAL
database under the Flink sink, and a `sqlite3` cursor held open across a 6 s warehouse
INSERT pins the read snapshot, which stops the WAL from being checkpointed while the
load runs. The loader therefore selects `LIMIT 769 … OFFSET > last row` per page and
runs it to completion before the INSERT: one extra cheap statement per batch, no long
lock.

**One statement per batch.** 769 rows is the ceiling `DBSQL` allows — 10,000 bound
parameters over thirteen columns — and 768 KiB of bound bytes is the second ceiling
(three quarters of the 1 MiB parameter limit; the limit counts UTF-8 bytes, not
characters). A row larger than the whole byte budget is written alone rather than
truncated.

**The schema mirrors SQLite verbatim.** Thirteen columns, counters as `BIGINT`/`INT`,
everything else `STRING`, `ingested_at` copied rather than restamped. Typing is dbt's
job in staging. The table is `CLUSTER BY (event_dt)` and `zstd`-compressed; the loader
creates it if it is absent, so a fresh workspace needs no manual DDL.

**Retention gaps are logged, not fatal.** If the target watermark sits below the
source's first offset for a partition, the loader warns with both ends of the missing
range and continues. It is a CLI tool; there is no `--allow-gap` to ask.

**The SQLite file is opened `mode=ro`.** It is live under the Flink sink. A wrong path
fails loudly instead of creating an empty database that reports zero rows.

## What it costs

Warehouse `5c5d597ca7af9460`: serverless **2X-Small**, `auto_stop_mins: 1`, list price
$0.70/DBU, 4 DBU/h while running. Two things follow, and they dominate the design:

1. **A statement costs real fixed time before it moves a byte.** Measured 2026-10-28
   from this loader's own INSERTs, seventeen timed batches between 7 KB and 767 KB of
   bound parameters: a full 769-row batch takes 5.4–7.3 s (mean 6.54 s), a 7 KB batch
   takes 2.7 s. Least squares gives **2.82 s fixed + 204 K bytes/s**, mean residual
   0.8 s. A `SELECT count(*)` of the same shape costs only 0.73 s — the extra ~2 s is
   binding ten thousand parameters and committing a Delta version.
2. **A run always pays a 60 s idle countdown.** `auto_stop_mins: 1` means the
   warehouse bills a minute after the last statement, so the floor per run is
   ~0.07 DBU (~$0.05) regardless of how little work it did — including a run that
   finds nothing pending.

That is the whole argument for batching. The 1.72 M-row backlog in the SQLite file
(1.85 GB of bound parameters) prices out as:

|Approach |Statements|Warehouse minutes|DBU|USD|
|---|---|---|---|---|
|One row per statement|1,724,597|~80,600|~5,375|~$3,763|
|769-row batches (this loader)|2,353|~262|~17.5|~$12.22|

~309× cheaper, and the difference is almost entirely round trips. Both columns come
from the same model; the batched figure was confirmed by `--dry-run --assume-empty`
against the live 2.34 GB file on 2026-10-28.

Three measured runs, for calibration: `--limit 3000` spent 45 s of INSERT time across
4 statements, `--limit 10000` spent 83 s across 14, and a second `--limit 3000` after
the paged read was introduced spent 34 s across 4 — ~$0.07–0.11 per 3,000–10,000 rows
including the countdown.

**Steady state is the other half of the story.** The model's ~191 rows/s (at ~1,066
characters a row) is the ceiling for this path. The Jetstream firehose ran at ~450
messages/s when measured — 1.62 M rows an hour — so an hourly incremental load needs
several hours per hour and can never catch up. This loader is a **backfill and
reconciliation tool**: run it after a sink restart, or on a schedule coarse enough that
a few hours of backlog is acceptable. Keeping the SQLite sink as the primary store is
the cheap answer.

Where the remaining money is: 2,353 statements × 2.8 s is ~110 of the 262 minutes —
fixed cost, not bytes. The `COPY INTO` path below removes it.

## The second path: `COPY INTO`

`copy_into.py` does the same job through a volume instead of the parameter channel:
export the unloaded rows to NDJSON, upload them, and land the whole directory with one
statement. There is no 10,000-parameter ceiling to pack against, so there is nothing to
batch.

**Measured end to end on 2026-10-28**, the whole 1.72 M-row backlog into a probe table
with the real DDL (`workspace.bluesky.copy_into_probe`, dropped afterwards):

|Stage|Time|Rate|
|---|---|---|
|Export to NDJSON (12 files, 2.257 GB)|45.7 s|49 MB/s|
|Upload to the volume|698.1 s|3.2 MB/s|
|`COPY INTO` (one statement)|37.5 s|60 MB/s|
|**Wall clock**|**13.0 min**| |
|**Billed warehouse time** (statement + 60 s countdown)|**97.5 s**|**~$0.076**|

1,724,597 rows landed, matching the source exactly: 1,724,597 distinct `offset`,
1,724,597 distinct `seq`, and 1,419,245,096 characters of `payload` — the same total
the source holds. Against the INSERT path's ~$12.22 that is **~161× cheaper**, and the
difference is entirely the 2,353 statements that are no longer sent.

The cost model fits the statement itself from three controlled sizes (24.7 MB, 149.8 MB,
534.7 MB): **3.30 s fixed + 126 MB/s**, residuals under 0.25 s. `--dry-run` uses that,
so it quotes ~$0.063 rather than the measured $0.076 — the gap is the cold start. The
warehouse idles out during the export, so the `COPY INTO` always runs against a stopped
warehouse: 4.63 s to connect cold against 1.82 s warm, measured separately.

**Upload is the bottleneck, and it is not billed.** The Files API moved 2.26 GB at
3.2 MB/s. That is single-threaded per file and round-trip bound, so bigger files do not
help; parallel uploads would, and are not worth the complexity for a backfill. Eleven
of the thirteen minutes are spent here, at zero DBU.

### What `COPY INTO` does not give you

**The guarantee is still at-least-once, for a different reason.** `COPY INTO` keeps a
load history keyed by **file path**, so re-running the same statement skips files it
already committed. That covers a retry. It does not dedupe **row content**: the same
rows under a new filename load again. The resume point is still the target's own
`MAX(offset)` per partition, read fresh at the start, so a run that committed before
losing its acknowledgement is not repeated — but a run that had already uploaded and
then re-ran under a new timestamp directory would duplicate. Key any model on
`(partition, offset)` or `seq`.

**The load history is per table and dies with it.** Drop and recreate `raw_bluesky` and
every file looks new. There is no local checkpoint to rebuild it from.

**Staging directories are cleared every run.** A run dying between upload and commit
orphans its files permanently; `clear_staging` removes every `load_*` directory at the
start of the next one. Anything not named `load_*` in the volume is left alone.

Two Files API facts make that clearing work, and both cost a debugging session:
`list_directory_contents` returns directory paths **with a trailing slash**, so a
prefix match on the last slash-separated segment silently finds nothing; and
`delete_directory` is **not recursive** — it raises `BadRequest: The directory is not
empty` — so the files have to be deleted first, deepest first.

**JSON inference types every number as `BIGINT`**, and `COPY INTO` refuses to merge
`BIGINT` into the `INT` column `partition` — `DELTA_FAILED_TO_MERGE_FIELDS`, measured
with plain inference, with `inferSchema = 'false'`, and with `schemaHints` naming the
type (that option is silently ignored for JSON). The statement therefore reads through
`FROM (SELECT …)`, casting each non-string column to its declared type; `copy_selection`
builds it from `RAW_COLUMNS`, so a retyped column cannot silently break the load.

### Which path to run

|Situation|Path|Why|
|---|---|---|
|A backlog, after a sink restart or a long outage|`copy_into`|One statement instead of thousands. ~161× cheaper.|
|A few thousand rows, hourly|`load_events`|Both pay the 60 s countdown (~$0.05) and dominate. `copy_into` adds 3–5 minutes of export and upload for no saving.|
|Roughly up to a few hundred thousand rows|either|At ~$0.06–0.10 they differ by pennies; pick on wall time.|

Neither keeps a local checkpoint, so they are interchangeable mid-backlog: run one, and
the other resumes from wherever the target got to.

## Testing

```bash
pytest                                  # offline: no warehouse, no fixture file
ruff format --check . && ruff check . && pyright
sqlfluff lint
```

The SQLite fixture is built per test in `tmp_path`; the warehouse is `tests/fakes.py`.
`test_cost_model.py` pins literal numbers — 62.8 s and $0.049 for a one-statement run,
1,301 and 5,087 statements for the two caps — rather than recomputing them from the
constants they are meant to guard, so a constant that drifts fails a test instead of
quietly mispricing a run. `test_load_events.py` covers the batching, the per-partition
resume, the oversized-row path, the `--assume-empty` refusal, and one non-obvious
mechanism: that a batch never leaves a SQLite read snapshot open (`busy == 0` on a
`wal_checkpoint(TRUNCATE)` taken while the loader is paused between batches).
`test_copy_into.py` covers the NDJSON framing (every column named, UTF-8 not escaped),
file splitting at the byte target, the `FROM (SELECT …)` projection and its casts, the
`--assume-empty` refusal, that a dry run writes nothing, that `clear_staging` removes
only `load_*` directories, and the copy cost arithmetic.
