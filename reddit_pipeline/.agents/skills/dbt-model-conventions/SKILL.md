---
name: dbt-model-conventions
description: Conventions for adding or changing dbt models in this Reddit pipeline — the per-source model layout, layer responsibilities and materialisation (including when a model overrides to incremental), stg_/int_/mart_ naming, source declarations (including source freshness via `ingested_at`), the project macros (`add_audit_fields()`, `window_hours()`), where column tests belong, the sqlfluff rules a new model must satisfy, and the known structural gaps that remain (the gaps the loaders' semantics leave open).
globs: ["models/**/*.sql", "models/**/*.yml", "macros/**/*.sql", "dbt_project.yml"]
---

# dbt model conventions

## Layers

| Layer | Materialisation | Responsibility |
| --- | --- | --- |
| `models/staging/reddit/` | view | rename, cast, dedupe raw source rows. One model per source table, `stg_<source>_<entity>.sql`; the dedupe is a total order (see Staging dedupe) |
| `models/intermediate/reddit/` | view | joins and derived columns, `int_<entity>_<verb>.sql` |
| `models/marts/reddit/` | table | analytics-ready aggregates, `mart_<entity>_<grain>.sql`. A model may override this to `incremental` when it genuinely needs it (see Incremental marts) |

Materialisation is set per layer in `dbt_project.yml` (`staging`, `intermediate`,
`marts`); the source subfolder inherits its layer's setting, so do not add
per-model `config(materialized=…)` unless a model genuinely departs from its
layer.

## Incremental marts

`models/marts/reddit/mart_comment_history.sql` is the one model that overrides the
`marts` `table` default, because it accumulates comment history rather than
recomputing an aggregate:

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
with its `r_worldnews` post context. Its rolling 5-hour window
(`comment_created_at >= current_timestamp() - interval {{ window_hours() }} hours`)
is wrapped in `{% if is_incremental() %}`, so a first/full build loads all history
and each later run merges only comments created in the last five hours. That
window mirrors the ingestion `INCREMENTAL_WINDOW_HOURS = 5`; the shared value
comes from the `window_hours()` macro (see Macros), so the SQL and the extractors
cannot drift apart silently.

## Staging dedupe

Each staging model collapses duplicate raw rows with a **total order**, so the
survivor is deterministic even when the source repeats a key:

```sql
row_number() over (partition by id order by created_utc desc, score desc) as rn
```

`score` is the tie-break for posts and comments, `link_karma` for users; the
outer query keeps `where rn = 1`. The tie-break matters because the loaders append
rows across runs — a windowed delete only removes the rows a run rewrites, so the
same `id` can legitimately appear more than once with an identical
`created_utc`.

## Sources

Declared in `models/staging/reddit/src_reddit.yml` as source `reddit_raw` — catalog
`workspace`, schema `topic_raw`:

| Table | Loaded by |
| --- | --- |
| `r_worldnews` | `ingestion/reddit/extract_reddit.py` |
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
All three marts — `mart_comment_history`, `mart_post_summary` and
`mart_user_metrics` — call it.

`.sqlfluff` sets `load_macros_from_path = macros` so sqlfluff resolves
`{{ add_audit_fields() }}` the same way dbt does. Without it, linting any model
that calls the macro fails on an undefined macro.

The macro body is indented 4 spaces inside the `{% macro %}` block because
sqlfluff `LT02` requires it; the `-%}` / `{%-` whitespace markers then strip that
indentation and the trailing newline, so the macro emits a bare expression and
inherits the caller's indentation.

`macros/window_hours.sql` defines `window_hours()`, returning `5` — the dbt-side
definition of the ingestion rolling window, used by `mart_comment_history` as
`interval {{ window_hours() }} hours`. It is a macro rather than a dbt `var` on
purpose: sqlfluff's jinja templater stubs `var()` with a placeholder token, which
makes that `interval` expression unparsable and fails `sqlfluff lint`. A macro
renders normally through `load_macros_from_path`. The number has no second
definition in `dbt_project.yml` — this macro is the single source of it.
`tests/test_window_contract.py` renders this macro and asserts it equals
`ingestion.reddit._common.INCREMENTAL_WINDOW_HOURS`, so the two languages cannot
drift silently.

## Tests

Column tests live in the `.yml` next to the models (`stg_reddit.yml`,
`marts.yml`). Current pattern is `unique` + `not_null` on each grain key:
`post_id`, `comment_id`, `user_id`, `author_username`. `mart_comment_history`
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
