-- Raw is append-only, so the same posting appears once per fetch it survived.
-- This model keeps the newest fetch of each (source, source_job_id) and shapes
-- the three sources' payloads into one schema.
with source as (

    select * from {{ source('jobs', 'raw_job_postings') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by source, source_job_id
            order by ingested_at desc
        ) as fetch_rank

    from source
    where source_job_id is not null

),

cleaned as (

    select
        -- Pass-throughs first, then the derived columns, so the select list
        -- reads in the order the raw columns arrive.
        source,
        source_job_id,
        url,
        published_at,
        ingested_at,
        concat_ws(':', source, source_job_id) as job_key,
        trim(title) as title,
        lower(trim(title)) as title_normalized,
        nullif(trim(company_name), '') as company_name,
        nullif(trim(location), '') as location,
        nullif(trim(job_type), '') as job_type,
        nullif(trim(industry), '') as industry,
        nullif(trim(salary_text), '') as salary_text,
        from_json(tags_json, 'array<string>') as tags,
        -- Databricks' regexp flavour accepts POSIX classes, so no backslashes
        -- are needed to collapse the HTML/whitespace in the description blobs.
        nullif(
            trim(regexp_replace(regexp_replace(description, '<[^>]*>', ' '), '[[:space:]]+', ' ')),
            ''
        ) as description_text

    from deduped
    where fetch_rank = 1

)

select * from cleaned
