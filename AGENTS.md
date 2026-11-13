# AGENTS.md

Orientation for the `AmanLP7/dbt` checkout: three Databricks pipelines sharing one
SQL warehouse, one credential flow, and one Python environment, plus a pair of Kafka
flows whose SQLite sinks feed one of them.

- **Hard invariants** live in `.omp/RULES.md`. OMP loads that file in every project
  directory (via symlinks); §7 summarizes it, the rules file is authoritative.
- **Per-project detail** is in each project's `README.md`.
- This file is the map: what lives where, how data moves, and which command runs what.

## 1. Warehouse access

Ad-hoc Databricks SQL goes through the MCP server `databricks`; the CLI and
`databricks-sql-connector` are fallback only. The invariants that govern this are in
`.omp/RULES.md` — the material below is context, not a second copy of the rules.

Workspace `dbc-280fcd4a-642e.cloud.databricks.com` (Express trial, AWS `us-west-2`,
workspace id `7474652715826449`). One SQL warehouse, `5c5d597ca7af9460`: serverless
2X-Small, `auto_stop_mins: 1`. That pairing is the cost floor — 4 DBU/hr while it
runs, and idle billing capped at about 0.07 DBU instead of 2.0 per session, at the
price of one ~5 s cold start after every idle gap.

The warehouse's workspace-level `STATEMENT_TIMEOUT` is **120 s** (set 2026-11-09;
`SET STATEMENT_TIMEOUT` on the warehouse confirms `120`). Anything slower is killed
with `Statement has timed out after 120 seconds`, `SQLSTATE 57KD0`. That is 3.2×
headroom over the slowest legitimate statement on record — the Bluesky `COPY INTO` at
37.5 s — and the `MCP` `timeout` of 120000 ms in `.omp/mcp.json` matches it. Raising it
means a full-replacement `PUT /api/2.0/sql/config/warehouses`, which must carry
`ansi_mode` or silently drops it (`POST` on that path returns 404).

| Schema | Holds | Written by |
| --- | --- | --- |
| `workspace.job_postings` | `raw_job_postings`, seed `role_keywords`, `stg_job_postings`, `mart_role_listings`, `mart_role_summary` | `job_postings` extractor, then `dbt build` |
| `workspace.reddit` | 3 staging views, 2 intermediate views, 8 mart tables (2 rebuilt per run, 1 incremental `merge` on `comment_id`, 5 daily-edition incremental `delete+insert`), 2 seeds (`sections`, `trending_stopwords`) | `reddit_pipeline` `dbt build` |
| `workspace.topic_raw` | `r_posts`, `r_comments`, `r_users` | `reddit_pipeline` extractors |
| `workspace.bluesky` | `raw_bluesky` — 13 columns mirroring `main.events` (counters `BIGINT`/`INT`, rest `STRING`), `CLUSTER BY (event_dt)`, zstd; source declaration in dbt, no model yet. Volume `staging` next to it, transient | either `bluesky` loader (`ingestion/bluesky/load_events.py` or `copy_into.py`), then `dbt build` |

There is no `workspace.wikimedia` on this workspace. The retired consumer's Delta table
belongs to the previous workspace and was not recreated; `kafka_streams/wikimedia/`
writes SQLite only, so nothing needed it.

The server is Databricks' own `system.ai.dbsql`, reached at
`https://dbc-280fcd4a-642e.cloud.databricks.com/ai-gateway/mcp-services/system.ai.dbsql`.
It advertises `execute_sql` and `poll_sql_result`. `execute_sql` can write, so the
read-only guarantee is the caller's identity rather than a tool name: `.omp/mcp.json`
declares a **stdio** server that runs `~/.config/omp/dbsql-mcp-bridge.py`, a relay that
forwards JSON-RPC frames to that URL carrying a `Bearer` header for the service principal
`omp-dbsql-ro` (applicationId `2848e97e-e449-400f-9088-c33275d535d0`), minted by
`~/.config/omp/dbsql-token.py`.

- That principal holds `USE_CATALOG` on `workspace`, `USE_SCHEMA` + `SELECT` on
  `workspace.{job_postings,reddit,topic_raw,bluesky}`, and `CAN_USE` on the warehouse. It holds
  no `CREATE`, `INSERT`, `UPDATE`, `DELETE`, or `MODIFY` anywhere, so every write path
  ends in `PERMISSION_DENIED` inside Unity Catalog.
- `system` / `system.ai` access is inherited from its `account users` membership. The
  workspace user is not a metastore admin, so those two grants cannot be issued directly
  and must not be attempted.
- The token comes from an OAuth client secret, not a PAT: `client_credentials` against
  `/oidc/v1/token` with scope `all-apis`, one-hour lifetime, cached in
  `~/.config/omp/.dbsql-token-cache` and re-minted on demand. The secret lives in
  `~/.config/omp/dbsql-client.json` (mode 600) and expires 2028-10-23. `create-obo-token`
  is blocked for this principal on this workspace; do not retry it.
- The relay holds the header in memory only until 5 minutes before its `exp`, and a
  `401` from upstream forces a fresh mint (disk cache bypassed) plus one retry. That
  indirection exists because OMP resolves a `!command` header value once and
  process-caches the string: pointed straight at the endpoint with
  `"Authorization": "!$HOME/.config/omp/dbsql-token.py"`, a session loses SQL an hour
  after process start, and neither `/mcp reconnect` nor `/mcp reload` re-runs the
  command — both replay the startup string and keep getting `401 access token expired`.
  `/mcp reload` is still what picks up a *change* to `mcp.json`.

An MCP write probe does not raise an HTTP error: `execute_sql` answers HTTP 200 with
`status.state: FAILED`, `error_code: BAD_REQUEST`, `sql_state: 42501`, and a
`PERMISSION_DENIED: Principal 'omp-dbsql-ro' does not have …` message, while `isError`
stays `false`. Read the body, not the status code.

The fallback identity is different: the Databricks user `ak547276@gmail.com` (CLI profile
`DEFAULT`) owns every schema in `workspace` and every table in them — recorded in the
`information_schema` owner columns, not as grants — including every dbt-produced
relation. The fallback channel can therefore write, which is why the write gate in
`.omp/RULES.md` is absolute; read there for the error taxonomy and fallback conditions.

Authentication is the Databricks CLI profile `DEFAULT`; an ambient `DATABRICKS_PAT`
is stale — do not use it, and `~/.databrickscfg` keeps `default_profile = DEFAULT` so a
bare `databricks` call resolves to this workspace rather than the old `FREE` one.

MCP registration is per working directory. OMP reads only the `.omp/` of its cwd and
does not walk ancestors, so each project carries symlinks to the root
`.omp/{mcp.json,config.yml,RULES.md}`. A new project directory needs the same three
links before a session starts inside it; links added mid-session take effect only
after a restart.

## 2. Repository map

| Path | What it is |
| --- | --- |
| `.omp/` | OMP settings shared by every project directory: `mcp.json` (the stdio entry that runs the relay), `config.yml` (access notes), `RULES.md` (hard invariants). The relay itself lives outside the checkout, at `~/.config/omp/dbsql-mcp-bridge.py`, beside the token script it calls. |
| `.venv/` | The only Python environment in the checkout; no project directory has one. |
| `AGENTS.md` | This orientation. |
| `dbt_readme.md` | Personal dbt notes. Gitignored; not authoritative for this repo. |
| `logs/` | Root dbt log output. Gitignored. |
| `bluesky/` | dbt project + the loader that copies the local Bluesky SQLite sink into `workspace.bluesky.raw_bluesky`. No Dagster code location. |
| `job_postings/` | dbt project + keyless job-board extractors + Dagster code location. |
| `reddit_pipeline/` | dbt project + Reddit extractors + Dagster code location + agent skills. |
| `kafka_streams/` | Kafka flows. Shared broker + shared Python config, one folder per flow. `wikimedia/` sinks the Wikimedia firehose into SQLite (Flink SQL job or Python consumer) and `bluesky/` sinks the Jetstream firehose (Flink SQL job only — its Python consumer was deleted). No dbt project. |

There is **no root-level dbt project**. `dbt` resolves `dbt_project.yml` from the
current directory, so run it from inside `job_postings/`, `reddit_pipeline/` or
`bluesky/`.

## 3. Data flow

```mermaid
flowchart LR
  subgraph JP[job_postings]
    J1[remotive / arbeitnow / jobicy] --> J2[(raw_job_postings)]
    J3[(role_keywords seed)] --> J5[mart_role_listings]
    J2 --> J4[stg_job_postings view] --> J5
    J5 --> J6[mart_role_summary]
  end
  subgraph RD[reddit_pipeline]
    R1[Reddit API via PRAW] --> R2[(topic_raw: r_posts, r_comments, r_users)]
    R2 --> R3[3 staging views] --> R4[2 intermediate views] --> R5[3 mart tables]
    R3 --> R6[5 daily-edition marts] --> R7[(edition archive, incremental by day)]
  end
  subgraph KS[kafka_streams]
    K1[Wikimedia NDJSON over HTTP] --> K2[wikimedia producer] --> K3[(wikimedia.recentchange)] --> K4[Flink job / consumer] --> K5[(wikimedia.db)]
    K6[Bluesky Jetstream over WebSocket] --> K7[bluesky producer] --> K8[(bluesky.jetstream)] --> K9[Flink job] --> K10[(bluesky.db)]
  end
  subgraph BS[bluesky]
    K10 --> B1[load_events.py, 769-row INSERT batches] --> B2[(workspace.bluesky.raw_bluesky)]
  end
```

The Wikimedia firehose used to be consumed straight into `workspace.wikimedia.raw_wikimedia`
by a top-level `wikimedia/` consumer. That consumer was removed on 2026-10-20 in favour of
`kafka_streams/wikimedia/`; the Delta table lives on the previous workspace only and was
not recreated here, and nothing writes it now.

Each pipeline is independently runnable: extractors and the consumer are plain Python
modules, and dbt runs from inside the project directory. Dagster wraps the same steps
in one ordered job and adds retries, asset checks, and (for `job_postings`) a stopped
daily schedule. `bluesky/` has no Dagster layer: its loaders are two CLIs over the
SQLite file `kafka_streams/bluesky/` writes, and they are the only path from that flow
into the warehouse.

## 4. Projects

### `job_postings/`

ELT over three keyless job-board APIs. The extractor normalises each board's payload
into the shared raw schema and applies no role filter; role matching is a SQL concern
applied in the marts against a seed.

| Path | Contents |
| --- | --- |
| `models/staging/jobs/` | `stg_job_postings` (view) — newest fetch per `(source, source_job_id)`, `job_key`, normalised title, unpacked tags, stripped HTML — plus `src_jobs.yml` (source `jobs.raw_job_postings`, 4 source tests) and `stg_jobs.yml` (3 model tests). |
| `models/marts/jobs/` | `mart_role_listings` and `mart_role_summary` (tables), 7 model tests, all declared in `marts.yml`. |
| `seeds/role_keywords.csv` | 11 keywords across 4 role families; the taxonomy the marts join against. |
| `ingestion/jobs/` | `fetch_job_postings.py` (remotive in one request; arbeitnow and jobicy paginated, with pacing between pages) and `_common.py` (env validation, DDL, chunked parameterised inserts). |
| `orchestration/` | Dagster code location `job_postings_orchestration.definitions`: job `job_board_refresh`, assets `jobs/raw_job_postings` + 4 dbt assets, 14 asset checks, schedule `daily_job_board_refresh` (cron `0 6 * * *`, ships stopped). |
| `tests/` | Offline pytest: extractor mappings, raw-table contract, seed contract, Dagster wiring, extractor asset. |
| `profiles/profiles.yml` | target `dev`, catalog `workspace`, schema `job_postings`. |

- `raw_job_postings` is append-only and stamped `ingested_at`; dedupe happens in
  staging on the newest row per `(source, source_job_id)`.
- Mart matching is a case-insensitive substring of the normalised title with no word
  boundary — `ai engineer` also matches "Dubai engineer". Wildcard characters in a
  keyword are rejected by `test_seed_contract.py`.
- `open_listings` means "postings seen and not re-listed differently", not "jobs open
  today": nothing expires from the raw table.

### `reddit_pipeline/`

ELT over every subreddit in `seeds/sections.csv` (66 today) — posts, comments, and
commenter profiles — plus a daily "newspaper" mart layer built from them.

| Path | Contents |
| --- | --- |
| `models/staging/reddit/` | `stg_reddit_posts`, `stg_reddit_comments`, `stg_reddit_users` (views), with `src_reddit.yml` and `stg_reddit.yml`. |
| `models/intermediate/reddit/` | `int_posts_enriched`, `int_comments_enriched` (views) — joins and business logic. |
| `models/marts/reddit/` | `mart_post_summary`, `mart_user_metrics` (tables), `mart_comment_history` (incremental, merge). All three call `add_audit_fields()`. |
| `models/marts/reddit/daily/` | The daily edition: `mart_daily_edition`, `mart_daily_section_summary`, `mart_daily_trending_terms`, `mart_daily_reading_list`, `mart_subreddit_daily_volume` — all incremental on `edition_date`, `delete+insert`, last three editions each run. Tests and descriptions in `daily.yml`. |
| `seeds/sections.csv` | `subreddit, section, weight` — the newspaper's section map. A subreddit the seed omits still prints, under `other`. |
| `seeds/trending_stopwords.csv` | Words dropped from titles before the trending mart counts them. |
| `ingestion/reddit/` | One module per entity plus `_common.py`: clients, env validation, table setup, the seed-driven subreddit list, and the SQLite dump + COPY INTO machinery; `copy_into.py` lands the dump in the raw tables. |
| `orchestration/` | Dagster code location `reddit_pipeline_orchestration.definitions`: job `reddit_refresh`, 17 assets (3 extractors + `copy_into` + 13 dbt), 70 asset checks, one schedule `daily_edition_refresh` (cron `0 5 * * *` UTC, ships stopped). |
| `macros/` | `add_audit_fields()`, `window_hours()` (the dbt-side 26-hour read window), `story_domain()` (outlet label from a post url). |
| `tests/` | Offline pytest: extractors, Dagster wiring, extractor assets, window contract. |
| `.agents/skills/` | Skills `lint-and-typecheck`, `dbt-model-conventions`, `reddit-extraction`; list them from this directory with `omp skill list`. |
| `profiles/profiles.yml` | target `dev`, catalog `workspace`, schema `reddit`. |

- Sources are `workspace.topic_raw.{r_posts,r_comments,r_users}`. Freshness
  thresholds (12 h warn, 24 h error on `ingested_at`) are declared but inert: no job
  or CI step runs `dbt source freshness`, and `dbt build` does not check freshness.
- **The subreddit list is a seed.** `subreddit_list()` reads `seeds/sections.csv` —
  the same file that maps a subreddit to its newspaper section — so adding a
  subreddit is one seed row and the ingest cannot drift from the marts. 66 rows
  today. Each subreddit costs two listing requests (`new(100)` and
  `top(100, day)`), deduped by post id, so ~132 requests per posts run against a
  6,000/hour budget. A subreddit that cannot be read is logged at `WARNING` and
  skipped, not fatal.
- **All three loads dump to SQLite, then `COPY INTO`.** Extractors write their
  fetched rows into a local SQLite run database (`data/reddit.db` by default, env
  `REDDIT_SQLITE_PATH`); `copy_into.py` (or the orchestrated `raw_copy_into` asset)
  exports the dump as NDJSON, uploads it to the Unity Catalog volume
  `workspace.topic_raw.staging` (env `REDDIT_VOLUME`), and runs append-only
  `COPY INTO` into the raw tables. No upsert or DELETE is ever needed: the staging
  models already dedupe by id, and `ingested_at` is cast back to `TIMESTAMP` by the
  COPY selection so source freshness still works. No delete is ever scoped by time:
  two daily runs through bounded listings cover different spans of the same day, so a
  `created_utc >= cutoff` delete would drop rows the run never refetched.
  `INCREMENTAL_WINDOW_HOURS = 26` is a **read** filter only. It is 26, not 24,
  because a daily cadence landing a minute late would otherwise leave a gap;
  `window_hours.sql` and the constant must move together or `test_window_contract.py`
  fails.
- Comments cover only the 200 most-commented in-window posts (comment fetching is
  one request per post); users are capped at 200 profiles, taken from post authors
  first and comment authors only if that falls short.
- `COPY INTO` is append-only and runs only after the dump is staged, so a failed
  load never removes a row that was already there; the staging dedupe absorbs any
  repeated ids.
- 4 of the 70 asset checks are `relationships` tests marked `severity: warn`, so a
  green build does not prove referential integrity.
- The daily marts are keyed on `edition_date` = the **story's own creation date**, not
  the build date, so a rebuild reproduces the same paper and the archive survives the
  raw tables rolling. Each run replaces the last three editions.
- Section membership is a seed join, never a `CASE` in SQL: adding a subreddit must
  stay a data change. Ranking is `score + 2 * comment_count`; Reddit's own decay
  ranking is not used, because in a daily edition it puts an hour-old 17-upvote post
  above the day's reporting.
- `parse_url(url, 'HOST')` requires the **upper-case** part name on this warehouse.
  Measured 2026-11-12: the lower-case `'host'` returns NULL for every url, which
  silently produced an all-null `domain` column and one failing `not_null` test.
- 05:00 UTC is the schedule. An edition is built from whatever is in `topic_raw` at
  build time, so the 05:00 run must follow a completed ingest; nothing enforces that
  beyond `reddit_refresh` running the extractors before `dbt build`.
- `mart_daily_reading_list` was empty while the ingest was r/worldnews alone (that
  subreddit is link-only; `length(trim(body)) > 0` counted zero rows on 2026-11-12).
  It fills from the text-heavy seeded subreddits, with a `word_count >= 250` floor.

### `bluesky/`

ELT over the Bluesky Jetstream firehose, via the local SQLite sink that
`kafka_streams/bluesky/` already writes. No Dagster code location and no schedule:
the loader is run by hand, after a sink restart or on a coarse cron.

| Path | Contents |
| --- | --- |
| `ingestion/bluesky/load_events.py` | CLI `--db --table --batch-rows --limit --max-seconds --dry-run --assume-empty`; prints the cost of the work before doing it and what it spent after. |
| `ingestion/bluesky/copy_into.py` | CLI `--db --table --volume --max-file-bytes --limit --dry-run --assume-empty --work-dir --keep-files`: export NDJSON, upload to the volume, one `COPY INTO`. |
| `ingestion/bluesky/_common.py` | Env validation, the SQL client, the target DDL, the read-only SQLite reader, paging, batching, both cost models. |
| `workspace.bluesky.staging` | Unity Catalog volume, the upload target for `COPY INTO`. Transient; `load_*` directories are cleared at the start of every run. |
| `models/staging/bluesky/src_bluesky.yml` | Source `bluesky_raw.raw_bluesky` with all 13 columns; freshness 6 h / 24 h on `cast(ingested_at as timestamp)`, verified against the warehouse. No model yet. |
| `tests/` | Offline pytest: batching, per-partition resume, gap warning, NDJSON framing, file splitting, staging hygiene, both cost models. SQLite is built per test in `tmp_path`; the warehouse and the volume are fakes. |
| `profiles/profiles.yml` | target `dev`, catalog `workspace`, schema `bluesky`. |

- **The resume point is read from the target**, `SELECT partition, MAX(offset) …
  GROUP BY partition`, not kept locally, so a re-run cannot disagree with what landed.
  It also means a completed load is a no-op that still pays the warehouse's idle
  minute.
- **Delivery is at-least-once.** The watermark advances only after an `INSERT`
  returns, so a batch the server committed whose acknowledgement was lost is re-sent
  and lands twice — at most 769 rows, invisible to `MAX(offset)`. Nothing dedupes:
  a model over this table must key on `(partition, offset)` or `seq`.
- **One `INSERT` per batch**, capped by both DBSQL limits: 769 rows (10,000 bound
  parameters over 13 columns) and 768 KiB of bound bytes (the parameter limit counts
  UTF-8 bytes). A row larger than the whole byte budget goes alone rather than being
  truncated.
- **A run loads the backlog it planned for, then stops.** Page reads resume past the
  previous page and the source is a live sink, so an unbounded drain would chase the
  firehose forever at ~191 rows/s against ~450 msg/s. The row budget is the
  `pending_stats` count from that run's own plan.
- **Each page is read to completion before it is inserted.** The source is a live WAL
  database; a cursor held open across a 6 s `INSERT` pins the read snapshot and stops
  the WAL being checkpointed. One extra `SELECT … LIMIT` per batch, no long lock.
- **The schema mirrors `main.events` verbatim**, `ingested_at` copied not restamped;
  typing is dbt's job. The loader creates the table if absent.
- **The SQLite file is opened `mode=ro`** — it is live under the Flink sink, and a
  wrong path must fail rather than create an empty database that reports zero rows.
- Every read is scoped to `topic = 'bluesky.jetstream'`. A retention gap (target
  watermark below the source's first offset) is logged with both ends and the load
  continues; there is no `--allow-gap`.
- `--dry-run` reads SQLite and the target and writes nothing. `--assume-empty` is
  accepted only with `--dry-run`: on a real run it would append the whole source a
  second time, and nothing downstream dedupes.
- `loaded_at_field` is `cast(ingested_at as timestamp)`. `ingested_at` lands as
  `STRING`, and an uncast column makes `dbt source freshness` raise
  `Expected a timestamp value … received value of type 'str'` — the 6 h/24 h
  thresholds are real here, not inert.

**There are two loaders, and the choice is arithmetic.** `load_events` pushes rows
through the SQL parameter channel at ~204 K/s with 2.8 s of fixed cost per statement;
`copy_into` writes NDJSON, uploads it, and lands the directory in one statement that
reads at 126 MB/s. Measured 2026-10-28 on the 1.72 M-row backlog: INSERT ~2,353
statements, ~262 min, ~$12.22; COPY INTO 12 files (2.257 GB), 45.7 s to export, 698.1 s
to upload, 37.5 s to copy, 13.0 min wall, 97.5 s billed, **~$0.076** — ~161× cheaper.
Both confirmed against a probe table with the real DDL.

- **`COPY INTO` needs `FROM (SELECT …)`, not a bare path.** JSON inference types every
  number as `BIGINT`, and `COPY INTO` will not merge `BIGINT` into the `INT` column
  `partition` (`DELTA_FAILED_TO_MERGE_FIELDS`). `inferSchema = 'false'` and
  `schemaHints` both fail; the latter is silently ignored for JSON. `copy_selection()`
  builds the cast projection from `RAW_COLUMNS`.
- **`COPY INTO`'s idempotence is per file path, not per row.** A retried statement
  skips committed files; the same rows under a new filename load again. The resume
  point is still the target's `MAX(offset)` per partition. The history is per table
  and dies with it.
- **Upload is the wall-clock bottleneck and costs nothing.** The Files API ran at
  3.2 MB/s, single-threaded per file, so 11 of the 13 minutes are unbilled.
- **`COPY INTO` always starts on a cold warehouse**, because the export idles it out:
  4.63 s to connect cold against 1.82 s warm.
- Only one loader writes at a time, and each run clears stale `load_*` staging
  directories first — a run dying between upload and commit orphans its files forever.
- **Two Files API quirks make that clearing work.** `list_directory_contents` returns
  directory paths with a trailing slash, so a prefix match on the last slash-separated
  segment matches nothing (measured: it turned `clear_staging` into a no-op); and
  `delete_directory` is not recursive — it raises `BadRequest: The directory is not
  empty` — so files are deleted first, deepest first.

### `kafka_streams/`

Kafka flows, independent of the dbt projects and of Databricks. The broker is
one container from `docker-compose.yml` (single-node KRaft, 512 MiB heap under a
1 GiB limit, advertised on `localhost:9092`, 7-day retention); `confluent-kafka`,
`requests`, `websockets`, and `python-dotenv` are installed once at the checkout
root; `.env`, `requirements*.txt`, and `pyproject.toml` live at `kafka_streams/`,
not per flow.

`kafka_streams/wikimedia/` is the first flow: `ingestion/wikimedia/producer.py`
bridges the public NDJSON firehose into `wikimedia.recentchange`, and two
interchangeable readers write that topic into `wikimedia/data/wikimedia.db`
(`main.events`, `main.kafka_offsets`) — `ingestion/wikimedia/flink_job.py`
(Flink SQL) and `ingestion/wikimedia/consumer.py` (Python). Progress is stored in
SQLite; Kafka's committed offsets are unused — never committed, and never a
fallback. One reader per database file, because SQLite WAL allows one writer, and
the Flink job pins parallelism to 1 for the same reason.

The Flink job needs a second environment, `.flink-venv` (`requirements-flink.txt`):
`apache-flink` pulls `apache-beam`, which requires `protobuf<6`, while dbt requires
`protobuf>=6`. Installing it into `.venv` breaks the dbt projects. It also needs a
JDK 17 (`JAVA_HOME`) and four jars in `wikimedia/lib/` — three from Maven Central
via `lib/fetch-jars.sh`, plus `lib/flink-connector-sqlite-4.1.0-2.2.jar` compiled
by `lib/build-sqlite-dialect.sh` from `lib/sqlite-dialect/`, because Flink ships
no SQLite `JdbcDialect`. The job is Flink SQL rather than a PyFlink dataflow: the
events table needs each message's `partition` and `offset`, which only the SQL
connector exposes as metadata columns.

The sink is at-least-once in both readers and deduped by `INSERT OR IGNORE` on
`event_id`. The Python consumer writes its cursor in the same transaction as the
rows; the Flink sink cannot, so `heal_cursor` derives it from `MAX(offset)+1` per
partition at startup and after a clean exit — forward only, so a crash costs a
replay, never a gap.

The Flink MiniCluster serves the stock Web Dashboard on a pinned REST port
(`WIKIMEDIA_FLINK_REST_PORT`, default 8081; it takes the next free port up to +9
and logs the URL). Opening it while the job runs gives the job graph, per-operator
counters, and the failing exception without re-running anything.

Two resume guards, shared via `_kafka.py`: a checkpoint records the Kafka
`cluster_id`, so a recreated broker resets instead of landing inside a fresh log;
and a stored offset below the log's earliest offset (retention deleted it) stops
the process rather than silently skipping the range — `--allow-gap` is the
explicit override. The broker writes to the `kafka_streams_data` volume
(`KAFKA_LOG_DIRS`), so `docker compose down` keeps the topic; `down -v` does not.
The bluesky flow adds a third: the Kafka *topic* id, which is the only thing that
catches a topic deleted and recreated on the same broker — the cluster id
survives that, and the new log restarts at 0 behind a checkpoint that still looks
plausible.

`kafka_streams/bluesky/` is the second flow, same shape one protocol over:
`producer.py` bridges the Jetstream v2 WebSocket (`wss://…/subscribeEvents`,
subprotocol `xrpc.v1.json`, mandatory) into `bluesky.jetstream`, and a Flink SQL
job writes that topic into `bluesky/data/bluesky.db`. This flow has one reader:
its Python consumer was deleted on 2026-10-28, and `flink_job.py` is the only
process that polls the topic. Three things differ from Wikimedia and all are
load-bearing:

- **`main.events` is keyed on `seq`, not on a record URI.** One record has several
  events (a `create`, then an `update` or `delete`); keying on the `at://` URI
  discarded 15 of the first 3,500 messages (0.43%). `record_key` remains as an
  indexed lookup column. `seq` is Jetstream's own per-event counter, so a replay
  still dedupes.
- **The producer keeps its own upstream position** in `main.jetstream_cursor`, the
  microsecond `time` of the newest event librdkafka acknowledged, persisted every
  500 events. Jetstream clamps a cursor older than ~36.9 h (measured: 132,742 s) to
  the oldest retained event instead of rejecting it, so the producer compares the
  first replayed event's age with the age the cursor implied and logs an ERROR when
  the server clamped. A clamped run cannot catch up — replay measured 451 events/s
  against the live 297/s — so rejoining the live edge needs `--live`.
- **Its sink uses a batching JDBC driver.** The stock `org.sqlite.JDBC` commits
  every statement of an `executeBatch()` separately, one WAL fsync each: 100 rows/s
  for a 500-row batch against 24,252 rows/s inside one transaction, measured
  2026-10-28. Flink's JDBC sink neither disables auto-commit nor offers an option
  for it, so `BatchedSqliteDriver` in this flow's `lib/sqlite-dialect/` wraps each
  batch in a transaction; `sink_ddl()` names it in the `'driver'` option. Without
  it the reader ran at 139–169 rows/s against a ~450 msg/s firehose and the backlog
  grew; with it the sink tracks the firehose (459 rows/s vs 448 msg/s at the live
  edge). Wikimedia still uses the stock driver.

The producer holds the same file lock as the reader, so it must be stopped (or
pointed at another `--db`) while the job starts. Its Flink dashboard defaults to
port 8082, so both flows' jobs can run at once.

Adding a flow means a new topic, a new SQLite file, and `<FLOW>_`-prefixed keys in
`kafka_streams/.env`; it does not mean a second broker.

## 5. Shared conventions

### dbt project shape

- One dbt project per pipeline; each `dbt_project.yml` names the project and its
  profile.
- Materialisation is keyed by layer directory in `dbt_project.yml` and inherited by
  source-named subfolders: `staging` → view, `intermediate` → view (reddit only),
  `marts` → table. A model can override it with `{{ config(...) }}`
  (`mart_comment_history` does, and all five in `marts/reddit/daily/` override to
  incremental). `bluesky` declares no layer config yet: it has only
  a source, and dbt warns about paths that configure nothing.
- Each layer keeps one subfolder per source — `jobs/`, `reddit/`, `bluesky/` — so a
  second source sits beside the first without changing the layer config.
- Sources are declared in `models/staging/<source>/src_*.yml`. `reddit_pipeline` and
  `bluesky` pin their schema; `job_postings` resolves `JOB_POSTINGS_CATALOG` /
  `JOB_POSTINGS_SCHEMA` with the target as fallback, matching the extractor.
- Seeds live in `job_postings` (`role_keywords`) and `reddit_pipeline` (`sections`,
  `trending_stopwords`). `analyses/`, `snapshots/`, and bluesky's `seeds/` are
  placeholders.
- Macros live in `macros/`; sqlfluff resolves them via `load_macros_from_path`.
- `tests/` is both dbt's singular-test directory and pytest's directory. dbt reads
  only `.sql` from it, so the Python suite is invisible to dbt.

### Python and credentials

- Python 3.10. Every dbt project and `kafka_streams/` configure ruff (line length 120)
  and pyright in `pyproject.toml`, and each dbt project carries a `.sqlfluff` for the
  `databricks` dialect.
- Install into the root `.venv`, never as `uv tool` copies — `~/.local/bin` precedes
  the pyenv shims and would shadow the environment's `dbt`/`python`.
- Configuration is per-project `.env`, copied from `.env.example` and gitignored.
  Extractors call `load_dotenv()`; **dbt does not read `.env`**.
- One `databricks auth login` covers everything. Extractors hand the SQL connector a
  credentials provider; Dagster mints `DATABRICKS_TOKEN` immediately before each dbt
  run; a manual `dbt` needs the token exported first.
- The ambient `DATABRICKS_PAT` is stale and rejected — do not use it.

### Profiles and dbt invocation

Every dbt project commits `profiles/profiles.yml` and carries a `profiles.yml` symlink
to it, so a bare `dbt` inside the project finds the repo profile instead of
`~/.dbt/profiles.yml` — which still describes the previous FREE workspace and an expired
literal PAT. Passing the directory explicitly works everywhere:

```bash
DBT_PROFILES_DIR=profiles dbt build
```

### Testing and CI

- Offline pytest (stubbed HTTP and warehouse) is the pre-merge gate. dbt's own tests
  need a warehouse and run inside `dbt build`.
- `job_postings` pytest reads `target/manifest.json`, so run `dbt parse` first on a
  fresh checkout. `reddit_pipeline`'s `conftest.py` parses the project itself; so does
  `bluesky`'s, and it stubs `load_dotenv` before the loader is imported.
- All three dbt projects carry `.github/workflows/ci.yml`; `kafka_streams/` has no
  workflow. GitHub Actions reads workflows only from the repository root
  `.github/workflows`, which this checkout does not have, so in the current layout
  these are local definitions rather than running CI.

## 6. Commands

Run from the project directory. `dbt build` writes to the warehouse; the write gate in
`.omp/RULES.md` governs hand-written statements, not the build itself.

| Task | `job_postings/` | `reddit_pipeline/` |
| --- | --- | --- |
| Install | `pip install -r requirements.txt -r requirements-dev.txt -e orchestration` | same |
| Parse (no warehouse) | `DBT_PROFILES_DIR=profiles dbt parse` | same |
| Build + test | `DBT_PROFILES_DIR=profiles dbt build` | same |
| Extract/load only | `python -m ingestion.jobs.fetch_job_postings` | `python -m ingestion.reddit.extract_reddit`, `…_comments`, `…_users`, then `python -m ingestion.reddit.copy_into` |
| Orchestrated run | `dagster asset materialize -m job_postings_orchestration.definitions --select '*'` | `dagster job execute -m reddit_pipeline_orchestration.definitions --job reddit_refresh` (NOT `asset materialize`: the ad-hoc job's multiprocess executor races on the shared SQLite dump) |
| UI + scheduler | `dagster dev` (from `orchestration/`) | `dagster dev` (from `orchestration/`) |
| Offline tests | `pytest` | `pytest` |
| Lint / types | `ruff format --check . && ruff check . && sqlfluff lint && pyright` | same |

`bluesky/`: `pip install -r requirements.txt -r requirements-dev.txt` (no orchestration
package), then either loader — `--dry-run --assume-empty` prices the backlog touching
only SQLite, `--limit N` proves the write path. Same `dbt parse` / `dbt build` /
`pytest` / lint commands as the other two.

| Task | `bluesky/` |
| --- | --- |
| Load, incremental (INSERT) | `python -m ingestion.bluesky.load_events` |
| Load, backlog (COPY INTO) | `python -m ingestion.bluesky.copy_into` |
| Price both paths | `python -m ingestion.bluesky.copy_into --dry-run --assume-empty` |

Run the loader from a shell whose `DATABRICKS_HOSTNAME` / `DATABRICKS_HTTP_PATH` match
profile `DEFAULT`, or with neither exported — see the gotcha about stale `FREE`
exports below, which breaks `OpenSession` before any SQL runs.

`kafka_streams/`: `pip install -r requirements.txt -r requirements-dev.txt`, then
`docker compose up -d` (needs the `docker` group), then from `wikimedia/` or
`bluesky/`: `python -m ingestion.<flow>.producer --create-topic`, `…producer`;
`pytest -q` and `ruff format --check . && ruff check . && pyright` (run from
`kafka_streams/`). A Python reader remains only in `wikimedia/`
(`…consumer`); `bluesky/` reads through its Flink job. The flows write SQLite
only; they touch no Databricks table and need no warehouse.

The Flink reader runs from a flow directory under the *other* environment, with a
JDK on the path: `./lib/fetch-jars.sh`, `./lib/build-sqlite-dialect.sh`, then
`JAVA_HOME=<jdk17> ../.flink-venv/bin/python -m ingestion.<flow>.flink_job`
(`--bounded` drains the backlog and exits). Each flow carries its own `lib/` copy,
so the jars are fetched and the dialect compiled per flow. Tests and lint stay
under `.venv`: `flink_job` imports PyFlink only inside `run()`.

## 7. Rules at a glance

The authoritative text is `.omp/RULES.md`. Summary:

| Rule | Applies to |
| --- | --- |
| Ad-hoc Databricks queries go through the MCP server `databricks` (`system.ai.dbsql`: `execute_sql`, `poll_sql_result`). CLI and connector are fallback only. | every manual query |
| A SQL-level `FAILED` means the query is wrong — fix it on MCP. `[OP_NOT_SUPPORTED_READ_ONLY]` means it was a write — stop. | interpreting MCP errors |
| A write on MCP answers HTTP 200 with `status.state: FAILED` and `PERMISSION_DENIED`; `isError` stays `false`. Read the body, not the status code. | MCP write probes |
| HTTP 403 is a denial to report. A 401 means the MCP credential path is broken — fix the credential or `/mcp reload`, never a CLI workaround. Neither licenses a CLI workaround. | transport errors |
| A fallback is read-only (`SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`) and is never run in parallel with MCP to compare. The one non-error reason: determining what the fallback identity itself may do. | fallback path |
| Never write to a relation `dbt build` produces. Any write needs an explicit yes for that exact statement. | writes |
| `databricks auth token --profile DEFAULT` is always allowed; the MCP path never uses it. | credential plumbing |

## 8. Gotchas

- `dbt show --inline` and `dbt run-operation` execute SQL through the write-capable
  profile — they are ad-hoc queries, not pipeline runs.
- Unqualified table names resolve differently by channel: MCP and the raw connector
  fall back to `system.session, system.builtin, system.ai, workspace.default`, while
  dbt uses the active profile's schema (`job_postings`, `reddit` or `bluesky`).
  Qualify names.
- `.venv` exists only at the checkout root; the project directories have none.
- The extractors dial whatever `DATABRICKS_HOSTNAME` the environment holds while
  minting the token from the `DATABRICKS_CONFIG_PROFILE` config. A shell that exports
  the `FREE` workspace's host with `DEFAULT` selected points the connection at a
  workspace the token was not issued for, and `OpenSession` fails with HTTP 400 and an
  empty `error-message` — no "unauthorized", no hint which side is wrong.
  `load_dotenv()` does not help: it never overrides an already-set variable, so the
  `.env` value loses to a stale export. Check `echo $DATABRICKS_HOSTNAME` against
  `databricks auth describe -p DEFAULT` before blaming the connector.
- **`ALTER TABLE … RENAME TO <bare name>` does not stay in the table's schema.** The
  target resolves against the statement session's default schema, which here is
  `workspace.default`, so renaming `workspace.topic_raw.r_worldnews` to `r_posts` moved
  it to `workspace.default.r_posts` and reported `SUCCEEDED` either way (measured
  2026-11-12). Always qualify the target three-part:
  `ALTER TABLE a.b.old RENAME TO a.b.new`. Verify with `SHOW TABLES IN` afterwards,
  not the statement status.
- `job_postings` pytest needs a prior `dbt parse`; reddit's does not.
- `bluesky`'s loader is the one component here that writes to the warehouse without
  `dbt build`: it appends to `workspace.bluesky.raw_bluesky` as the CLI user. That is
  a pipeline run (the write gate governs hand-written statements), but anything that
  rewrites or deletes from that table by hand still needs an explicit yes.
- Both Dagster schedules ship stopped — `daily_job_board_refresh` (06:00 UTC) and
  `daily_edition_refresh` (05:00 UTC) — and `bluesky` has no orchestration. Nothing
  writes to the warehouse on its own until someone flips a schedule in the UI.
- `dagster dev` sets `DAGSTER_IS_DEV_CLI`, so the code location's `prepare_if_dev()`
  runs `dbt parse` at import and the profile's `env_var(...)` values must be present
  then. Outside `dagster dev`, `job_postings` never parses on import: its pytest and
  `dagster definitions validate` need a prior `dbt parse`. `reddit_pipeline` parses
  on import only when `target/manifest.json` is missing.
