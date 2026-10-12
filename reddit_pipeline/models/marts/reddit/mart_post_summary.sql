with posts as (
    select * from {{ ref('int_posts_enriched') }}
),

comments as (
    select * from {{ ref('stg_reddit_comments') }}
),

comment_aggregates as (
    select
        post_id,
        count(*) as comments_collected,
        coalesce(avg(score), 0) as avg_comment_score,
        coalesce(max(score), 0) as max_comment_score
    from comments
    group by post_id
)

select
    p.post_id,
    p.title,
    p.post_score,
    p.post_url,
    p.comment_count as total_comments_on_reddit,
    p.post_created_at,
    p.post_content,
    p.author_username as post_author,
    p.author_link_karma as post_author_link_karma,
    p.author_comment_karma as post_author_comment_karma,
    p.author_is_gold as post_author_is_gold,
    p.subreddit,
    coalesce(c.comments_collected, 0) as comments_collected_locally,
    coalesce(c.avg_comment_score, 0.0) as avg_comment_score_locally,
    coalesce(c.max_comment_score, 0) as max_comment_score_locally,
    {{ add_audit_fields() }}
from posts p
left join comment_aggregates c on p.post_id = c.post_id
