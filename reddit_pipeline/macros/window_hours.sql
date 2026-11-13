{#
    The ingestion read window, in hours, as a dbt-side definition.

    `mart_comment_history` replicates the ingestion window in SQL; the extractors
    define theirs in `ingestion/reddit/_common.py` (`INCREMENTAL_WINDOW_HOURS`).
    On both sides it is a read filter, not a delete scope: the extractors dump
    append-only (the dbt staging models dedupe by id), so re-reading 26 hours
    costs a dedupe, not a re-download.
    26 hours, not 24: the pipeline runs once a day and a 24-hour window landing a
    minute late would leave a gap between consecutive runs.
    `tests/test_window_contract.py` renders this macro and asserts the two agree,
    so the two languages cannot drift apart silently.

    It is a macro rather than a dbt `var` on purpose: sqlfluff's jinja templater
    stubs `var()` with a placeholder token, which makes
    `interval {{ var('window_hours') }} hours` unparsable and fails `sqlfluff
    lint`. A macro renders normally under `.sqlfluff`'s `load_macros_from_path`.
#}
{% macro window_hours() -%}
    26
{%- endmacro %}
