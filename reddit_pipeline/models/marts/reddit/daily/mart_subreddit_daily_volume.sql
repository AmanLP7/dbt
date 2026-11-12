{{ config(materialized='incremental', unique_key='edition_date', incremental_strategy='delete+insert') }}

{#
    One row per (day, subreddit). Two jobs at once: it is the app's "what's busy"
    panel, and it is the pipeline's own heartbeat — a subreddit that drops to zero
    rows on a day the edition ran means its extractor is broken, not that the
    subreddit went quiet.
#}
with posts as (
    select * from {{ ref('stg_reddit_posts') }}
    {% if is_incremental() %}
        where cast(created_at as date) >= date_sub(current_date(), 2)
    {% endif %}
),

daily as (
    select
        cast(created_at as date) as edition_date,
        subreddit,
        count(*) as post_count,
        sum(comment_count) as total_comments,
        sum(score) as total_score,
        approx_percentile(score, 0.5) as median_score,
        max(score) as top_score,
        min(created_at) as first_post_at,
        max(created_at) as last_post_at
    from posts
    group by cast(created_at as date), subreddit
)

select
    concat(cast(edition_date as string), '|', subreddit) as subreddit_day_key,
    edition_date,
    subreddit,
    post_count,
    total_comments,
    total_score,
    median_score,
    top_score,
    first_post_at,
    last_post_at,
    -- Rate over the span the posts actually cover, floored at one hour so a single
    -- post in a day reads as a rate rather than a division by zero.
    round(post_count / greatest(
        timestampdiff(minute, first_post_at, last_post_at) / 60.0, 1.0
    ), 2) as posts_per_hour,
    {{ add_audit_fields() }}
from daily
