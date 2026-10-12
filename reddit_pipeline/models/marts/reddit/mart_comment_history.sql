{{
    config(
        materialized='incremental',
        unique_key='comment_id',
        incremental_strategy='merge',
    )
}}

-- Comment-grain history combining r_comments (via int_comments_enriched) with its
-- r_worldnews post context. The rolling window mirrors INCREMENTAL_WINDOW_HOURS in
-- the ingestion scripts: a full refresh rebuilds all history, while an incremental
-- run only merges comments created in the last five hours.
with comments_enriched as (
    select * from {{ ref('int_comments_enriched') }}
),

posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

recent_comments as (
    select *
    from comments_enriched
    {% if is_incremental() %}
        where comment_created_at >= current_timestamp() - interval {{ window_hours() }} hours
    {% endif %}
)

select
    c.comment_id,
    c.post_id,
    p.title as post_title,
    p.subreddit,
    p.score as post_score,
    p.url as post_url,
    p.comment_count as post_comment_count,
    c.comment_content,
    c.comment_score,
    c.author_username,
    c.author_id,
    c.author_link_karma,
    c.author_comment_karma,
    c.author_is_employee,
    c.author_is_gold,
    c.comment_created_at,
    c.parent_id,
    {{ add_audit_fields() }}
from recent_comments c
left join posts p on c.post_id = p.post_id
