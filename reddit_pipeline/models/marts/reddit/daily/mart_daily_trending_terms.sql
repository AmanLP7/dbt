{{ config(materialized='incremental', unique_key='edition_date', incremental_strategy='delete+insert') }}

{#
    Which words are rising. One row per (edition_date, term), with the term's own
    previous week as its baseline.

    The baseline is read from `mart_daily_edition`, which keeps every edition, so a
    term's history is available even though the raw tables only hold the last few
    days. A `range between interval 7 days preceding and interval 1 day preceding`
    window is used rather than a row-count window: a term that was silent yesterday
    has no row for yesterday, and a row-count window would average only the days it
    did appear, inflating every ratio.
#}
with edition as (
    select
        edition_date,
        post_id,
        subreddit,
        title
    from {{ ref('mart_daily_edition') }}
    -- Nine days: the day being scored plus the seven-day baseline behind it, plus a
    -- day of slack so a late-arriving post does not rewrite an already-published
    -- edition's baseline.
    where edition_date >= date_sub(current_date(), 9)
),

stopwords as (
    select stopword from {{ ref('trending_stopwords') }}
),

-- One row per (story, word). The title is lowercased *before* punctuation is
-- stripped: the other order lets `[^a-z0-9 ]` eat every capital letter, which
-- turned `Russia` into `ussia` and made the two spellings count as different terms.
-- The stopword seed carries the grammar; the length floor is doing something
-- narrower, and the measured token counts say so. At three characters the
-- survivors are mostly contraction debris and filler — `don` (76 occurrences,
-- from "don't", whose apostrophe the strip turns into a space), `day` (55),
-- `old`, `big`, `end`, `til` — so a floor of four keeps far more noise out than
-- it removes. It does cost real three-letter subjects: `ceo` (49), `war` (45),
-- `nba`, `nfl`, `mma`. Grow the stopword seed if those should trend; do not
-- lower the floor without re-reading the top of the three-character list.
words as (
    select
        edition_date,
        post_id,
        subreddit,
        term
    from edition
        lateral view explode(
            split(regexp_replace(lower(title), '[^a-z0-9 ]', ' '), ' ')
        ) t as term
),

filtered as (
    select w.*
    from words w
    left join stopwords s on w.term = s.stopword
    where
        s.stopword is null
        and length(w.term) > 3
        and w.term rlike '^[a-z][a-z0-9]*$'
),

daily_terms as (
    select
        edition_date,
        term,
        count(distinct post_id) as post_count,
        count(distinct subreddit) as distinct_subreddits
    from filtered
    group by edition_date, term
),

with_baseline as (
    select
        edition_date,
        term,
        post_count,
        distinct_subreddits,
        sum(post_count) over (
            partition by term
            order by cast(edition_date as timestamp)
            range between interval 7 days preceding and interval 1 day preceding
        ) as trailing_posts
    from daily_terms
)

select
    concat(cast(edition_date as string), '|', term) as term_key,
    edition_date,
    term,
    post_count,
    distinct_subreddits,
    coalesce(trailing_posts, 0) as baseline_posts,
    round(coalesce(trailing_posts, 0) / 7.0, 2) as baseline_posts_per_day,
    -- A term absent from the baseline week gets a one-post-per-day floor, so its
    -- ratio is its own count instead of a division by zero.
    round(post_count / greatest(coalesce(trailing_posts, 0) / 7.0, 1.0), 2) as rise_ratio,
    {{ add_audit_fields() }}
from with_baseline
-- A single mention a day is noise; two is a topic. The threshold is what keeps the
-- trending strip short enough for the app to show without scrolling.
where
    post_count >= 2
    {% if is_incremental() %}
        and edition_date >= date_sub(current_date(), 2)
    {% endif %}
