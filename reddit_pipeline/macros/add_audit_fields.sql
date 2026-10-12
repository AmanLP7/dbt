{#
    Audit columns for a model's table.

    Append it to the select list to stamp the row with an audit timestamp:

        select
            *,
            {{ add_audit_fields() }}
        from {{ ref('some_model') }}

    `rec_updated_at` is emitted as a TIMESTAMP. Under a `table` materialization it
    records the time the row was written by the dbt run; under a `view` the
    expression is evaluated per query, so it reports query time, not load time.

    Add further audit columns to the list below; every model that calls the macro
    picks them up. The `-%}` / `{%-` markers strip the surrounding whitespace so the
    macro emits one bare expression and inherits the caller's indentation.
#}
{% macro add_audit_fields() -%}
    cast(current_timestamp() as timestamp) as rec_updated_at
{%- endmacro %}
