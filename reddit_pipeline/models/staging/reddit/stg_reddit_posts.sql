with source as (
    select * from {{ source('reddit_raw', 'r_worldnews') }}
),

cleaned as (
    select
        id as post_id,
        title,
        score,
        url,
        comms_num as comment_count,
        cast(from_unixtime(created_utc) as timestamp) as created_at,
        body as post_content,
        author as author_username,
        subreddit,
        row_number() over (partition by id order by created_utc desc, score desc) as rn
    from source
)

select
    post_id,
    title,
    score,
    url,
    comment_count,
    created_at,
    post_content,
    author_username,
    subreddit
from cleaned
where rn = 1
