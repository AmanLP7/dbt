with source as (
    select * from {{ source('reddit_raw', 'r_users') }}
),

cleaned as (
    select
        id as user_id,
        username as author_username,
        cast(from_unixtime(created_utc) as timestamp) as created_at,
        link_karma,
        comment_karma,
        is_employee,
        is_gold,
        row_number() over (partition by id order by created_utc desc, link_karma desc) as rn
    from source
)

select
    user_id,
    author_username,
    created_at,
    link_karma,
    comment_karma,
    is_employee,
    is_gold
from cleaned
where rn = 1
