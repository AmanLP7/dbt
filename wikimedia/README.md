# Wikimedia lambda architecture

A lambda architecture over the Wikimedia EventStreams `recentchange` firehose:

| Layer | Status | What it is |
| --- | --- | --- |
| Speed | **built** | `ingestion/wikimedia/consume_recentchange.py` tails the SSE firehose and micro-batches into `workspace.wikimedia.raw_wikimedia` |
| Batch | not built | dbt models that reprocess the same raw table over full history |
| Serving | not built | incremental marts on top of the batch layer |

## Tooling decisions

Chosen against what the workspace actually has, not from a reference diagram.

| Choice | Reason |
| --- | --- |
| Wikimedia EventStreams (SSE), not a Kafka topic | the public firehose needs no auth and no broker to operate |
| A Python SSE consumer, not Spark Structured Streaming | the workspace has **no classic clusters** — only a stopped *Serverless Starter Warehouse* (2X-Small) — so there is no Spark compute to run `readStream` on. A consumer process plus SQL `INSERT` needs only the warehouse that already exists. |
| `databricks-sql-connector` micro-batch `INSERT` | same pattern as the sibling `reddit_pipeline`: `ensure_table` + parameterised bulk insert, no cluster, no Auto Loader volume |
| Unity Catalog catalog `workspace`, schema `wikimedia` | matches the existing `workspace.reddit` / `workspace.topic_raw` layout |
| `databricks auth login` profile for credentials | one login covers ingestion and, later, the dbt batch layer; no PAT in `.env` |

The trade this makes explicit: micro-batching over a SQL endpoint gives seconds of
latency (per flush), not milliseconds. A true speed layer with sub-second latency
would need Kafka plus Spark compute, neither of which exists here.

## Raw table

`workspace.wikimedia.raw_wikimedia`

| Column | Type | Meaning |
| --- | --- | --- |
| `payload` | `STRING` | the event JSON **exactly as it arrived** |
| `ingested_at` | `TIMESTAMP` | load time, UTC, identical for every row of one flush |

**The table is append-only.** Every flush is an `INSERT`; there is no delete,
merge, update or dedupe. Consequences, all deliberate:

- history is cumulative and immutable, so the batch layer can recompute over the
  full table under new logic without a re-ingest;
- a restarted stream that replays a batch it already wrote costs duplicate rows
  rather than lost ones — dedupe belongs downstream, keyed on `meta.id`;
- the whole event is kept, so a field no layer knows about yet (`meta.dt`,
  `wiki`, `namespace`, `bot`, `length`, `revision`, `server_url`, …) is not lost.

## Running it

```bash
pip install -r requirements.txt
cp .env.example .env          # fill in DATABRICKS_HOSTNAME and DATABRICKS_HTTP_PATH
databricks auth login --profile DEFAULT

python -m ingestion.wikimedia.consume_recentchange                # until stopped
python -m ingestion.wikimedia.consume_recentchange --max-events 200   # bounded run
```

The consumer creates the catalog, schema and table on first run if they do not
exist, so no manual DDL is needed.

## How the consumer behaves

- **Micro-batching** — flushes at 500 rows or 15 seconds, whichever comes first
  (`--flush-every` overrides the row count).
- **Resumable** — the SSE `id:` field of each committed batch is written to
  `.state/last_event_id.json` and replayed as the `Last-Event-ID` header on the
  next connect. The checkpoint is written *after* the insert, so it can only ever
  trail the data, never run ahead of it.
- **Reconnects** — any transport error reconnects on a 1/5/15/30/60s ladder,
  resuming from the last committed id.
- **Shutdown** — `SIGINT`/`SIGTERM` flushes the buffer and exits rather than
  dropping the pending batch.
- **Bad frames** — a frame whose payload is not valid JSON is logged and dropped
  instead of being written for every later reader to trip over.

## Layout

```
ingestion/wikimedia/_common.py              env validation, Databricks client, append-only load, checkpoint
ingestion/wikimedia/consume_recentchange.py the SSE consumer (speed layer)
tests/test_consume_recentchange.py          offline tests: framing, batching, resume, reconnect
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
ruff check .
```

`iter_sse_frames` takes an iterable of lines and `consume` takes its cursor,
sleep function and stop predicate as arguments, so the suite exercises framing,
batching, checkpoint ordering, resume and reconnects with no HTTP and no
warehouse.
