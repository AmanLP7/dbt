"""Extract open job listings from keyless job APIs into the raw Delta table.

Run from the project root:

    python -m ingestion.jobs.fetch_job_postings

None of these three sources honours a role/keyword query (``?search=`` is
accepted and then ignored), so the extractor takes the board and normalises it to
the shared raw schema. Picking a role is a dbt concern, applied downstream
against the raw table — never a filter here, so raw stays faithful.

arbeitnow and jobicy paginate and cap their page size server-side (arbeitnow
serves 300 per page and ignores ``per_page``; jobicy honours ``count=100``), so
one request can never return either board whole — arbeitnow's runs to ~95 pages.
Both order newest first, so the extractor walks up to ``MAX_PAGES_PER_SOURCE``
pages of each and treats that cap as the recency window. Remotive does not
paginate and is fetched in a single request.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from ingestion.jobs import _common

LOGGER = logging.getLogger(__name__)

USER_AGENT = "job-postings-elt/0.1"
REQUEST_TIMEOUT_SECONDS = 30

REMOTIVE_URL = "https://remotive.com/api/remote-jobs?limit=100"
ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"
JOBICY_URL = "https://jobicy.com/api/v2/remote-jobs?count=100"

# How many pages of each paginated source one run reads. Both boards order newest
# first and neither accepts a row-count limit (arbeitnow ignores `per_page`), so
# this constant *is* the recency window: 5 pages is the newest ~1400 arbeitnow and
# 500 jobicy postings. Raising it buys history at one request per page — the raw
# table is append-only, so every extra page is re-inserted on every run.
MAX_PAGES_PER_SOURCE = 5

# Pause between page requests. arbeitnow answers a burst with HTTP 429 and asks
# callers not to abuse the free API, so the walk paces itself rather than
# hammering five requests back to back.
PAGE_REQUEST_DELAY_SECONDS = 1.0


def _get_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        return json.load(response)


def _paged(first_url: str, next_page_of: Callable[[dict[str, Any]], str | None]) -> list[dict[str, Any]]:
    """Fetch up to `MAX_PAGES_PER_SOURCE` pages, pausing between requests.

    Stops early when a payload reports no next page, so a board shorter than the
    cap costs only the requests it actually needs. When the cap is what stopped
    the walk, that is logged: silently truncating a firehose is the kind of thing
    worth knowing about.
    """
    payloads: list[dict[str, Any]] = []
    url: str | None = first_url
    while url and len(payloads) < MAX_PAGES_PER_SOURCE:
        if payloads:
            time.sleep(PAGE_REQUEST_DELAY_SECONDS)
        payloads.append(_get_json(url))
        LOGGER.info("Fetched page %d of %s", len(payloads), url)
        url = next_page_of(payloads[-1])

    if url:
        LOGGER.warning(
            "Stopped at the %d-page cap with more of %s still available; raise MAX_PAGES_PER_SOURCE to read deeper",
            MAX_PAGES_PER_SOURCE,
            first_url,
        )
    return payloads


def _next_arbeitnow_page(payload: dict[str, Any]) -> str | None:
    """arbeitnow hands the next page's URL over whole; null once the board ends."""
    return (payload.get("links") or {}).get("next")


def _next_jobicy_page(payload: dict[str, Any]) -> str | None:
    cursor = payload.get("nextCursor")
    if not payload.get("hasMore") or not cursor:
        return None
    return f"{JOBICY_URL}&cursor={quote(str(cursor), safe='')}"


def _text(value: Any) -> str | None:
    """Trimmed string, or None for blanks — keeps empty strings out of raw."""
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _join_list(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, str):
        return _text(value)
    joined = ", ".join(str(item).strip() for item in value if str(item).strip())
    return joined or None


def _tags_json(value: Any) -> str | None:
    """Tags as a JSON array string; staging casts it back with from_json()."""
    if not value:
        return None
    items = [value] if isinstance(value, str) else [str(item) for item in value]
    return json.dumps([item for item in items if item])


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse the assorted date shapes these APIs emit into a naive UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(tzinfo=None)

    candidate = str(value).strip().replace("Z", "+00:00")
    parsers: tuple[Callable[[str], datetime], ...] = (
        datetime.fromisoformat,
        lambda text: datetime.strptime(text, "%Y-%m-%d %H:%M:%S"),
    )
    for parser in parsers:
        try:
            parsed = parser(candidate)
        except ValueError:
            continue
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    LOGGER.warning("Unparsable timestamp %r; storing null", value)
    return None


def fetch_remotive() -> list[dict[str, Any]]:
    payload = _get_json(REMOTIVE_URL)
    return [
        {
            "source": "remotive",
            # _text(), never str(): str(None) is the *string* "None", which would
            # pass the raw not_null test and collapse every id-less posting into
            # one deduped row.
            "source_job_id": _text(job.get("id")),
            "title": _text(job.get("title")),
            "company_name": _text(job.get("company_name")),
            "location": _text(job.get("candidate_required_location")),
            "job_type": _text(job.get("job_type")),
            "industry": _text(job.get("category")),
            "salary_text": _text(job.get("salary")),
            "tags_json": _tags_json(job.get("tags")),
            "description": job.get("description"),
            "url": _text(job.get("url")),
            "published_at": _parse_timestamp(job.get("publication_date")),
        }
        for job in payload.get("jobs", [])
    ]


def fetch_arbeitnow() -> list[dict[str, Any]]:
    postings = []
    for payload in _paged(ARBEITNOW_URL, _next_arbeitnow_page):
        for job in payload.get("data", []):
            location = _text(job.get("location")) or ("Remote" if job.get("remote") else None)
            postings.append(
                {
                    "source": "arbeitnow",
                    "source_job_id": _text(job.get("slug")),
                    "title": _text(job.get("title")),
                    "company_name": _text(job.get("company_name")),
                    "location": location,
                    "job_type": _join_list(job.get("job_types")),
                    "industry": None,
                    "salary_text": None,
                    "tags_json": _tags_json(job.get("tags")),
                    "description": job.get("description"),
                    "url": _text(job.get("url")),
                    "published_at": _parse_timestamp(job.get("created_at")),
                }
            )
    return postings


def fetch_jobicy() -> list[dict[str, Any]]:
    postings = []
    for payload in _paged(JOBICY_URL, _next_jobicy_page):
        for job in payload.get("jobs", []):
            postings.append(
                {
                    "source": "jobicy",
                    "source_job_id": _text(job.get("id")),
                    "title": _text(job.get("jobTitle")),
                    "company_name": _text(job.get("companyName")),
                    "location": _text(job.get("jobGeo")),
                    "job_type": _join_list(job.get("jobType")),
                    "industry": _join_list(job.get("jobIndustry")),
                    "salary_text": None,
                    "tags_json": _tags_json(job.get("jobIndustry")),
                    "description": job.get("jobDescription"),
                    "url": _text(job.get("url")),
                    "published_at": _parse_timestamp(job.get("pubDate")),
                }
            )
    return postings


SOURCES: tuple[tuple[str, Callable[[], list[dict[str, Any]]]], ...] = (
    ("remotive", fetch_remotive),
    ("arbeitnow", fetch_arbeitnow),
    ("jobicy", fetch_jobicy),
)


def extract_job_postings() -> int:
    """Fetch every source, append the rows to raw, and return the row count.

    One source failing (network, shape change, unparsable payload) costs only that
    source: it is logged and skipped, and the run aborts solely when no source
    produced anything. Any exception is treated that way, because a source whose
    payload shape changed raises from inside the fetcher, not as a URLError — and
    that must not throw away the rows the other two sources returned.
    """
    env = _common.require_env()
    collected: list[dict[str, Any]] = []
    failed: list[str] = []

    for name, fetcher in SOURCES:
        try:
            rows = fetcher()
        except Exception as exc:
            LOGGER.warning("Source %s failed and was skipped: %s", name, exc)
            failed.append(name)
            continue
        # raw_job_postings has a not_null test on source_job_id and staging dedupes
        # on it, so a posting the source published without a key cannot be
        # represented: drop it here instead of writing a row dbt rejects.
        keyed = [row for row in rows if row["source_job_id"]]
        if len(keyed) != len(rows):
            LOGGER.warning("Skipped %d postings from %s without a source_job_id", len(rows) - len(keyed), name)
        LOGGER.info("Fetched %d postings from %s", len(keyed), name)
        collected.extend(keyed)

    if len(failed) == len(SOURCES):
        raise RuntimeError("Every source failed: " + ", ".join(failed))

    with _common.connect(env) as connection, connection.cursor() as cursor:
        _common.ensure_raw_table(cursor)
        written = _common.insert_postings(cursor, collected)

    LOGGER.info("Wrote %d postings to %s", written, _common.raw_table_fqn())
    return written


def main() -> None:
    _common.configure_logging()
    extract_job_postings()


if __name__ == "__main__":
    main()
