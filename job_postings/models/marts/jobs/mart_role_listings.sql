-- The role filter lives here, not in the extractor: raw holds every listing the
-- sources returned, and this model narrows that to the roles in the
-- role_keywords seed. One posting can match several role families.
with listings as (

    select * from {{ ref('stg_job_postings') }}

),

keywords as (

    select
        role_family,
        lower(trim(keyword)) as keyword
    from {{ ref('role_keywords') }}

),

role_matches as (

    select
        keywords.role_family,
        listings.job_key,
        listings.source,
        listings.source_job_id,
        listings.title,
        listings.company_name,
        listings.location,
        listings.job_type,
        listings.salary_text,
        listings.tags,
        listings.url,
        listings.published_at,
        listings.description_text

    from listings
    inner join keywords
        on listings.title_normalized like '%' || keywords.keyword || '%'

)

select distinct
    role_family,
    job_key,
    source,
    source_job_id,
    title,
    company_name,
    location,
    job_type,
    salary_text,
    tags,
    url,
    published_at,
    description_text,
    {{ add_audit_fields() }}
from role_matches
