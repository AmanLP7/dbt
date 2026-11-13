---
name: dbt-model-conventions
description: Conventions for adding or changing dbt models in this Reddit pipeline — the per-source model layout, layer responsibilities and materialisation (including the six models that override to incremental), stg_/int_/mart_ naming, the daily edition family and its seeds (`sections`, `trending_stopwords`), source declarations (including source freshness via `ingested_at`), the project macros (`add_audit_fields()`, `window_hours()`, `story_domain()`), where column and seed tests belong, the sqlfluff rules a new model must satisfy, and the known structural gaps that remain (the gaps the loaders' semantics leave open).
globs: ["models/**/*.sql", "models/**/*.yml", "macros/**/*.sql", "dbt_project.yml"]
---

# dbt model conventions

## Layers

| Layer | Materialisation | Responsibility |
| --- | --- | --- |
| `models/staging/reddit/` | view | rename, cast, dedupe raw source rows. One model per source table, `stg_<source>_<entity>.sql`; the dedupe is a total order (see Staging dedupe) |
| `models/intermediate/reddit/` | view | joins and derived columns, `int_<entity>_<verb>.sql` |
| `models/marts/reddit/` | table | analytics-ready aggregates, `mart_<entity>_<grain>.sql`. A model may override this to `incremental` when it genuinely needs it (see Incremental marts) |
| `models/marts/reddit/daily/` | table, overridden to incremental | the once-a-day newspaper — see Daily edition |

Materialisation is set per layer in `dbt_project.yml` (`staging`, `intermediate`,
`marts`); the source subfolder inherits its layer's setting, so do not add
per-model `config(materialized=…)` unless a model genuinely departs from its
layer.

## Incremental marts

Six models override the `marts` `table` default. `mart_comment_history` is the
one that accumulates rather than recomputing, so it merges on `comment_id`:

```sql
{{
    config(
        materialized='incremental',
        unique_key='comment_id',
        incremental_strategy='merge',
    )
}}
```

It is comment-id grain, combining `r_comments` (via `int_comments_enriched`)
with its `r_posts` post context. Its 26-hour window
(`comment_created_at >= current_timestamp() - interval {{ window_hours() }} hours`)
is wrapped in `{% if is_incremental() %}`, so a first/full build loads all history
and each later run merges only comments created in the last 26 hours. That window
mirrors the ingestion `INCREMENTAL_WINDOW_HOURS = 26`; the shared value comes from
the `window_hours()` macro (see Macros), so the SQL and the extractors cannot drift
apart silently. The window is a *read* filter on both sides — the extractors dump
append-only and never delete by time, and the staging models dedupe by id, so
re-reading 26 hours is a cheap dedupe rather than a re-download.

## Daily edition

`models/marts/reddit/daily/` is a second mart layer, one subfolder deeper, whose five
models are all `incremental` on `edition_date` with `delete+insert`:

```sql
{{ config(materialized='incremental', unique_key='edition_date', incremental_strategy='delete+insert') }}
```

`delete+insert` rather than `merge` is deliberate. The grain is `(edition_date, …)`
and a story that falls out of the top 12 on a rebuild would otherwise sit at its old
rank forever, because a merge never deletes the rows that stopped qualifying. Each
model filters `edition_date >= date_sub(current_date(), 2)` inside
`{% if is_incremental() %}` — three days, so a late-arriving post corrects the last
two editions and nothing older.

`edition_date` is the **story's own creation date**, never the build date. A rebuild
therefore reproduces the same paper instead of shifting every story into today,
which is what makes the archive stable enough for the app to page backwards through.
It also means the models are correct as soon as the extractors are pointed at more
subreddits: no backfill exists for a day the pipeline did not run, and none is faked.

Sections come from `seeds/sections.csv` (`subreddit, section, weight`), joined on
`lower(posts.subreddit) = sections.subreddit`, with `coalesce(section, 'other')`. The
seed is **also the ingest list** — `subreddit_list()` in `ingestion/reddit/_common.py`
reads the same file — so the two cannot drift; a subreddit the seed does not name can
still print under `other` if a post reaches `r_posts` some other way. The `weight`
column is currently unused by every model. Never write the mapping as a `CASE` in
SQL: adding a subreddit must stay a data change.

Ranking is `score + 2 * comment_count`. Reddit's own `hot` decay is explicitly not
used: in a daily edition it promoted a 17-upvote post from an hour ago above the
day's actual reporting. `macros/story_domain.sql` provides the outlet label, and
collapses a self post's own reddit.com permalink to the empty string.

`mart_daily_reading_list` reads `stg_reddit_posts`, not `mart_daily_edition`, because
the edition stores a 280-character excerpt and a word count taken from that excerpt
would cap every story at ~45 words. It is one of two daily models that do not
descend from the edition — `mart_subreddit_daily_volume` also reads
`stg_reddit_posts`, because a per-subreddit post count cannot come from an edition
that keeps twelve stories a section. It was empty while the ingest was r/worldnews alone — that
subreddit is link-only and carries no self posts at all (`length(trim(body)) > 0`
counted zero rows, measured 2026-11-12). The ingest now covers the whole seed, whose
text-heavy subreddits (`askreddit`, `nostupidquestions`, `amitheasshole`,
`personalfinance`, …) do carry self posts, so the mart fills from the first daily run
that loads them. A `word_count >= 250` threshold decides what qualifies.

`mart_daily_trending_terms` lowercases a title **before** stripping punctuation:

```sql
split(regexp_replace(lower(title), '[^a-z0-9 ]', ' '), ' ')
```

The other order silently destroys every capital letter, so `Russia` became `ussia`
and the two spellings split into separate terms — measured on the first build. The
baseline is a `range between interval 7 days preceding and interval 1 day preceding`
window over `cast(edition_date as timestamp)`, not `rows`: a term silent yesterday
has no row at all, and a `rows` window would average only the days it appeared and
inflate every ratio.

`story_domain()` calls `parse_url(url, 'HOST')` — upper-case part name. The
lower-case `'host'` returns NULL for every url on this warehouse (measured
2026-11-12), which produces an all-null `domain` column without raising anything.

## Staging dedupe

Each staging model collapses duplicate raw rows with a **total order**, so the
survivor is deterministic even when the source repeats a key:

```sql
row_number() over (partition by id order by created_utc desc, score desc) as rn
```

`score` is the tie-break for posts and comments, `link_karma` for users; the
outer query keeps `where rn = 1`. The tie-break matters because the loaders are
at-least-once: a delete-then-insert whose acknowledgement is lost is retried, so the
same `id` can legitimately appear more than once with an identical `created_utc`.
The `id IN (...)` delete makes the duplicate unexpected, not impossible.

## Sources

Declared in `models/staging/reddit/src_reddit.yml` as source `reddit_raw` — catalog
`workspace`, schema `topic_raw`:

| Table | Loaded by |
| --- | --- |
| `r_posts` | `ingestion/reddit/extract_reddit.py` |
| `r_comments` | `ingestion/reddit/extract_reddit_comments.py` |
| `r_users` | `ingestion/reddit/extract_reddit_users.py` |

Reference them with `{{ source('reddit_raw', 'r_…') }}`, never a hardcoded
three-part name.

The source declares freshness and documents the load-time column:

```yaml
config:
  loaded_at_field: ingested_at
  freshness:
    warn_after: {count: 12, period: hour}
    error_after: {count: 24, period: hour}
```

Both settings sit under `config:` because dbt 1.12 deprecates the top-level
source properties. `ingested_at` is the trailing `TIMESTAMP` column the extractors
stamp at load time (`load_timestamp()` in `ingestion/reddit/_common.py`) — not a
value Reddit supplied — so it doubles as the freshness heartbeat for all three
raw tables. Each table's `ingested_at` is also declared in the source yml with a
description and `data_type: TIMESTAMP`.

## Macros

Reusable macros live in `macros/` (`macro-paths: ["macros"]` in `dbt_project.yml`),
outside `models/` — dbt claims model files under its model paths, and a macro is
not a model. Macros are auto-loaded, so call them bare.

`macros/add_audit_fields.sql` defines `add_audit_fields()`, which appends the
`rec_updated_at` audit column (`cast(current_timestamp() as timestamp)`) to a
model's select list. Under the `marts` `table` materialization it records write
time; under a `view` layer it is evaluated per query, so it reports query time.
All eight marts call it: the three under `models/marts/reddit/` and the five under
`models/marts/reddit/daily/`.

`.sqlfluff` sets `load_macros_from_path = macros` so sqlfluff resolves
`{{ add_audit_fields() }}` the same way dbt does. Without it, linting any model
that calls the macro fails on an undefined macro.

The macro body is indented 4 spaces inside the `{% macro %}` block because
sqlfluff `LT02` requires it; the `-%}` / `{%-` whitespace markers then strip that
indentation and the trailing newline, so the macro emits a bare expression and
inherits the caller's indentation.

`macros/window_hours.sql` defines `window_hours()`, returning `26` — the dbt-side
definition of the ingestion read window, used by `mart_comment_history` as
`interval {{ window_hours() }} hours`. It is a macro rather than a dbt `var` on
purpose: sqlfluff's jinja templater stubs `var()` with a placeholder token, which
makes that `interval` expression unparsable and fails `sqlfluff lint`. A macro
renders normally through `load_macros_from_path`. The number has no second
definition in `dbt_project.yml` — this macro is the single source of it.
`tests/test_window_contract.py` renders this macro and asserts it equals
`ingestion.reddit._common.INCREMENTAL_WINDOW_HOURS`, so the two languages cannot
drift silently.

## Tests

Column tests live in the `.yml` next to the models — `stg_reddit.yml`, `marts.yml`,
`marts/reddit/daily/daily.yml` — and seed tests in `seeds/seeds.yml`, which is the
only place a `seed` gets tests. Current pattern is `unique` + `not_null` on each
grain key: `post_id`, `comment_id`, `user_id`, `author_username`, and on the daily
marts' `story_key`, `section_key`, `term_key`, `reading_key` and
`subreddit_day_key`. `mart_comment_history`
asserts its comment-id grain with `unique` + `not_null` on `comment_id`, plus
`not_null` on `post_id`, `comment_created_at` and `rec_updated_at`, and a
`relationships` test `post_id -> ref('stg_reddit_posts').post_id` held at
`severity: warn`; `mart_post_summary` and `mart_user_metrics` each add a
`rec_updated_at` `not_null`. Generic-test arguments are nested under
`arguments:` (dbt 1.12 deprecates passing them as siblings).

The staging layer gained `not_null` on `stg_reddit_posts.author_username` and
`stg_reddit_comments.author_username` — the author columns the intermediate
models join on — plus three `relationships` tests, all at `severity: warn`:

| From | To |
| --- | --- |
| `stg_reddit_comments.post_id` | `stg_reddit_posts.post_id` |
| `stg_reddit_comments.author_username` | `stg_reddit_users.author_username` |
| `stg_reddit_posts.author_username` | `stg_reddit_users.author_username` |

They are `warn`, not `error`, because the ingestion window can legitimately load
a comment for a post/user the fetch did not return, so referential integrity is
not guaranteed. Warn keeps the gap visible without blocking a build.

`mart_post_summary`'s comment aggregate uses `count(*)` (aliased
`comments_collected`), not `count(<col>)`: the grouping key is `post_id` on rows
from `stg_reddit_comments`, where `post_id` is `not_null`, so the two agree and
`count(*)` states the intent — number of locally collected comments.

## Known structural gaps

- **Author columns hold `'[deleted]'`, not NULL.** `NOT NULL` passes on
  `author_username`, but the loaders write the literal `'[deleted]'` when
  `submission.author` is falsy, so `author_username` is not a reliable join to
  `r_users` even where it passes. Treat `'[deleted]'` as a sentinel in downstream
  logic.
- **Referential integrity is only warned on.** The four `relationships` tests
  above are `warn`-severity by design (see Tests), so a broken reference will not
  fail a build.

## Constraints a new model must satisfy

`sqlfluff lint` (no path; it covers `models/` and `macros/`) must stay green.
Practical checklist:

- lower-case keywords, types, and function names (`from_unixtime`, `coalesce`)
- a blank line after every CTE's closing `)`
- implicit table aliases (`from posts p`), consistent with all existing models
- lines ≤ 120 characters
- avoid `references.keywords` hits outside the ignored `source, users, title` —
  e.g. do not alias a CTE `values` or `order`
