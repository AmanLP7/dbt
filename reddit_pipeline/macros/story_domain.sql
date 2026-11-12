{#
    The publishing domain of a story link, reduced to its first label.

    A post's `url` is either an external link or — for a self post — the post's own
    permalink on reddit.com. `parse_url(..., 'HOST')` returns the host with no port.
    The part name must be upper-case: measured against this warehouse on 2026-11-12,
    `parse_url(url, 'host')` returns NULL for every url while `'HOST'` returns the
    host, so a lower-case part silently produces an all-null column. Dropping a
    leading `www.` and keeping the first dot-separated label then turns
    `https://www.bbc.com/news/...` into `bbc` and `https://www.pravda.com.ua/...`
    into `pravda`, which is the granularity a front page wants: "which outlet", not
    "which edition of which outlet".

    A self post has no outlet, so `reddit.com` and an unparseable url both collapse
    to the empty string — the honest answer for text the newsroom wrote itself.
    Reddit-hosted media (`i.redd.it`, `v.redd.it`) is labelled `redd.it` rather than
    taking the first label, which would print "i" or "v" as the outlet. Callers get
    a non-null column, so a `not_null` test means "the expression ran", not "every
    story has a link".

    The `coalesce` is load-bearing, not defensive: a non-null url with no parseable
    host (a relative permalink such as `/r/Film/comments/…`, which real rows carry)
    reaches the `else` branch with a null host, and every step after it propagates
    the null. Without it the `not_null` test on the edition's `domain` fails the
    build on data the pipeline is already seeing.
#}
{% macro story_domain(column) -%}
    coalesce(
        case
            when {{ column }} is null then ''
            when lower(parse_url({{ column }}, 'HOST')) in ('reddit.com', 'www.reddit.com') then ''
            when lower(parse_url({{ column }}, 'HOST')) = 'redd.it' then 'redd.it'
            when lower(parse_url({{ column }}, 'HOST')) like '%.redd.it' then 'redd.it'
            else split_part(regexp_replace(parse_url({{ column }}, 'HOST'), '^www\\.', ''), '.', 1)
        end,
        ''
    )
{%- endmacro %}
