with source as (
    select * from {{ source('reddit_raw', 'r_comments') }}
),

cleaned as (
    select
        id as comment_id,
        post_id,
        body as comment_content,
        score,
        author as author_username,
        cast(from_unixtime(created_utc) as timestamp) as created_at,
        parent_id,
        row_number() over (partition by id order by created_utc desc, score desc) as rn
    from source
)

select
    comment_id,
    post_id,
    comment_content,
    score,
    author_username,
    created_at,
    parent_id
from cleaned
where rn = 1
