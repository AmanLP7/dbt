with comments as (
    select * from {{ ref('stg_reddit_comments') }}
),

posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

users as (
    select * from {{ ref('stg_reddit_users') }}
)

select
    c.comment_id,
    c.post_id,
    p.title as post_title,
    c.comment_content,
    c.score as comment_score,
    c.author_username,
    c.created_at as comment_created_at,
    c.parent_id,
    u.user_id as author_id,
    u.link_karma as author_link_karma,
    u.comment_karma as author_comment_karma,
    u.is_employee as author_is_employee,
    u.is_gold as author_is_gold
from comments c
left join posts p on c.post_id = p.post_id
left join users u on c.author_username = u.author_username
