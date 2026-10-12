with posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

users as (
    select * from {{ ref('stg_reddit_users') }}
)

select
    p.post_id,
    p.title,
    p.score as post_score,
    p.url as post_url,
    p.comment_count,
    p.created_at as post_created_at,
    p.post_content,
    p.author_username,
    p.subreddit,
    u.user_id as author_id,
    u.link_karma as author_link_karma,
    u.comment_karma as author_comment_karma,
    u.is_employee as author_is_employee,
    u.is_gold as author_is_gold
from posts p
left join users u on p.author_username = u.author_username
