{#
    The ingestion rolling window, in hours, as a dbt-side definition.

    `mart_comment_history` replicates the ingestion window in SQL; the extractors
    define theirs in `ingestion/reddit/_common.py` (`INCREMENTAL_WINDOW_HOURS`).
    `tests/test_window_contract.py` renders this macro and asserts the two agree,
    so the two languages cannot drift apart silently.

    It is a macro rather than a dbt `var` on purpose: sqlfluff's jinja templater
    stubs `var()` with a placeholder token, which makes
    `interval {{ var('window_hours') }} hours` unparsable and fails `sqlfluff
    lint`. A macro renders normally under `.sqlfluff`'s `load_macros_from_path`.
#}
{% macro window_hours() -%}
    5
{%- endmacro %}
