{% macro add_audit_fields() -%}
    current_timestamp() as rec_updated_at
{%- endmacro %}
