with posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

comments as (
    select * from {{ ref('stg_reddit_comments') }}
),

users as (
    select * from {{ ref('stg_reddit_users') }}
),

user_post_stats as (
    select
        author_username,
        count(post_id) as total_posts,
        coalesce(sum(score), 0) as total_post_score,
        coalesce(avg(score), 0) as avg_post_score
    from posts
    group by author_username
),

user_comment_stats as (
    select
        author_username,
        count(comment_id) as total_comments,
        coalesce(sum(score), 0) as total_comment_score,
        coalesce(avg(score), 0) as avg_comment_score
    from comments
    group by author_username
)

select
    u.user_id,
    u.author_username,
    u.created_at as user_created_at,
    u.link_karma,
    u.comment_karma,
    u.is_employee,
    u.is_gold,
    coalesce(p.total_posts, 0) as total_posts,
    coalesce(p.total_post_score, 0) as total_post_score,
    coalesce(p.avg_post_score, 0.0) as avg_post_score,
    coalesce(c.total_comments, 0) as total_comments,
    coalesce(c.total_comment_score, 0) as total_comment_score,
    coalesce(c.avg_comment_score, 0.0) as avg_comment_score,
    {{ add_audit_fields() }}
from users u
left join user_post_stats p on u.author_username = p.author_username
left join user_comment_stats c on u.author_username = c.author_username
