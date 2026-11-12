{{ config(materialized='incremental', unique_key='edition_date', incremental_strategy='delete+insert') }}

{#
    One row per story in a day's edition, ranked inside its own section.

    The edition date is the story's creation date, not the build date, so a rebuild
    reproduces the same paper instead of shifting every story into today. That is
    what makes the archive stable enough for the app to page backwards through.

    `engagement` counts a comment as two upvotes: a post 200 people argued about is
    a bigger story than one 50 scrolled past. Reddit's own `hot` ranking is not used
    — it is a recency decay, and in a daily paper it promoted a 17-upvote post from
    an hour ago above the day's actual reporting.

    Sections come from the `sections` seed, not from a CASE in this SQL, so adding a
    subreddit is a data change. A subreddit the seed does not name lands in `other`
    rather than disappearing.

    Incremental on `edition_date` with delete+insert: the raw tables roll, so a
    plain table build would erase yesterday's paper, while a row-level merge would
    leave a story that fell out of the top 12 sitting at its old rank forever. Each
    run replaces the last three editions whole; older editions are never touched.
#}
with posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

sections as (
    select * from {{ ref('sections') }}
),

scored as (
    select
        cast(p.created_at as date) as edition_date,
        coalesce(s.section, 'other') as section,
        p.post_id,
        p.title,
        p.url,
        {{ story_domain('p.url') }} as domain,
        p.subreddit,
        p.author_username,
        p.score as post_score,
        p.comment_count,
        p.score + 2 * p.comment_count as engagement,
        left(trim(p.post_content), 280) as excerpt,
        p.created_at as post_created_at
    from posts p
    left join sections s on lower(p.subreddit) = s.subreddit
    {% if is_incremental() %}
        where cast(p.created_at as date) >= date_sub(current_date(), 2)
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by edition_date, section
            order by engagement desc, post_id asc
        ) as section_rank
    from scored
)

select
    concat(cast(edition_date as string), '|', post_id) as story_key,
    edition_date,
    section,
    section_rank,
    post_id,
    title,
    url,
    domain,
    subreddit,
    author_username,
    post_score,
    comment_count,
    engagement,
    excerpt,
    post_created_at,
    {{ add_audit_fields() }}
from ranked
-- Twelve stories a section is a newspaper page, not a dump: the app renders these
-- in order and never needs the tail, while the raw rows stay in staging.
where section_rank <= 12
