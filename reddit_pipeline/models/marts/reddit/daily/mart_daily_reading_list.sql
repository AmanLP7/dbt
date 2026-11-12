{{ config(materialized='incremental', unique_key='edition_date', incremental_strategy='delete+insert') }}

{#
    The day's long reads: self posts with enough text to be worth opening.

    It reads the raw staging table rather than the edition, because the edition
    stores a 280-character excerpt and a word count taken from a truncation would
    cap every story at ~45 words.

    Grain is (edition_date, reading_rank); `reading_key` is `edition_date|post_id`,
    which is unique per day because rank is a row_number over one edition. The
    model is incremental on the date for the same reason as the edition: the raw
    window rolls, the archive does not.
#}
with posts as (
    select * from {{ ref('stg_reddit_posts') }}
    {% if is_incremental() %}
        where cast(created_at as date) >= date_sub(current_date(), 2)
    {% endif %}
),

sections as (
    select * from {{ ref('sections') }}
),

long_posts as (
    select
        cast(p.created_at as date) as edition_date,
        p.post_id,
        p.title,
        p.subreddit,
        p.author_username,
        p.score as post_score,
        p.comment_count,
        p.post_content,
        p.created_at as post_created_at,
        coalesce(s.section, 'other') as section,
        p.score + 2 * p.comment_count as engagement,
        size(split(trim(p.post_content), ' ')) as word_count
    from posts p
    left join sections s on lower(p.subreddit) = s.subreddit
    where length(trim(p.post_content)) > 0
),

readable as (
    select *
    from long_posts
    -- Below ~250 words a self post is a question or a rant, not something to read.
    where word_count >= 250
),

ranked as (
    select
        *,
        row_number() over (
            partition by edition_date
            order by engagement desc, post_id asc
        ) as reading_rank
    from readable
)

select
    concat(cast(edition_date as string), '|', post_id) as reading_key,
    edition_date,
    reading_rank,
    section,
    post_id,
    title,
    subreddit,
    author_username,
    word_count,
    post_score,
    comment_count,
    engagement,
    left(trim(post_content), 600) as excerpt,
    post_created_at,
    {{ add_audit_fields() }}
from ranked
where reading_rank <= 10
