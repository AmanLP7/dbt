{{ config(materialized='incremental', unique_key='edition_date', incremental_strategy='delete+insert') }}

with edition as (
    select * from {{ ref('mart_daily_edition') }}
    {% if is_incremental() %}
        where edition_date >= date_sub(current_date(), 2)
    {% endif %}
),

summarised as (
    select
        edition_date,
        section,
        count(*) as story_count,
        sum(engagement) as total_engagement,
        sum(post_score) as total_score,
        sum(comment_count) as total_comments,
        sum(case when domain = '' then 0 else 1 end) as linked_story_count
    from edition
    group by edition_date, section
),

lead_story as (
    select
        edition_date,
        section,
        title as lead_story_title,
        url as lead_story_url,
        domain as lead_story_domain
    from edition
    where section_rank = 1
)

select
    concat(cast(s.edition_date as string), '|', s.section) as section_key,
    s.edition_date,
    s.section,
    s.story_count,
    s.linked_story_count,
    s.total_engagement,
    s.total_score,
    s.total_comments,
    t.lead_story_title,
    t.lead_story_url,
    t.lead_story_domain,
    {{ add_audit_fields() }}
from summarised s
left join lead_story t on s.edition_date = t.edition_date and s.section = t.section
