-- Demand per role family across every source, for the dashboards/reporting that
-- sit on top of the pipeline.
with summary as (

    select
        role_family,
        count(distinct job_key) as open_listings,
        count(distinct company_name) as hiring_companies,
        count(distinct source) as sources,
        min(published_at) as first_published_at,
        max(published_at) as latest_published_at

    from {{ ref('mart_role_listings') }}
    group by role_family

)

select
    summary.*,
    {{ add_audit_fields() }}
from summary
