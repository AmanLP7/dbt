"""Behaviour of the three normalisers, timestamp parsing, and the fail-soft loop.

Everything here runs offline: the HTTP layer is replaced by a stub and the
warehouse by `tests.fakes.FakeConnection`.
"""

from __future__ import annotations

import logging
import urllib.error
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from ingestion.jobs import _common
from ingestion.jobs import fetch_job_postings as extractor
from tests.fakes import FakeConnection, raw_row

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_SPEC = REPO_ROOT / "models" / "staging" / "jobs" / "src_jobs.yml"

ALL_RAW_KEYS = set(_common.RAW_COLUMNS) - {"ingested_at"}


def stub_json(monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any]) -> None:
    monkeypatch.setattr(extractor, "_get_json", lambda _url: payload)


# --- per-source normalisation -------------------------------------------------


def test_remotive_maps_its_payload_to_the_raw_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_json(
        monkeypatch,
        {
            "job-count": 1,
            "jobs": [
                {
                    "id": 1234567,
                    "url": "https://remotive.com/remote-jobs/data/data-engineer-1234567",
                    "title": "  Senior Data Engineer  ",
                    "company_name": "Acme",
                    "category": "Software Development",
                    "tags": ["python", "sql", ""],
                    "job_type": "full_time",
                    "publication_date": "2026-10-08T21:47:32",
                    "candidate_required_location": "Europe",
                    "salary": "OTE $25k - $35k",
                    "description": "<p>Build <b>pipelines</b></p>",
                }
            ],
        },
    )

    (posting,) = extractor.fetch_remotive()

    assert posting == {
        "source": "remotive",
        "source_job_id": "1234567",
        "title": "Senior Data Engineer",
        "company_name": "Acme",
        "location": "Europe",
        "job_type": "full_time",
        "industry": "Software Development",
        "salary_text": "OTE $25k - $35k",
        "tags_json": '["python", "sql"]',
        "description": "<p>Build <b>pipelines</b></p>",
        "url": "https://remotive.com/remote-jobs/data/data-engineer-1234567",
        "published_at": datetime(2026, 10, 8, 21, 47, 32),
    }
    assert set(posting) == ALL_RAW_KEYS


def test_arbeitnow_lists_are_joined_and_blank_location_becomes_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_json(
        monkeypatch,
        {
            "data": [
                {
                    "slug": "senior-data-engineer-acme",
                    "company_name": "Acme",
                    "title": "Senior Data Engineer",
                    "description": "<p>ELT</p>",
                    "location": "",
                    "remote": True,
                    "job_types": ["full-time", "permanent"],
                    "tags": ["python"],
                    "created_at": 1759960052,
                    "url": "https://www.arbeitnow.com/view/senior-data-engineer-acme",
                },
                {
                    "slug": "onsite-role",
                    "company_name": "Beta",
                    "title": "Data Analyst",
                    "description": "",
                    "location": "  ",
                    "remote": False,
                    "job_types": [],
                    "tags": [],
                    "created_at": None,
                    "url": None,
                },
            ]
        },
    )

    remote, onsite = extractor.fetch_arbeitnow()

    assert remote["location"] == "Remote"
    assert remote["job_type"] == "full-time, permanent"
    assert remote["industry"] is None
    assert remote["salary_text"] is None
    assert remote["published_at"] == datetime.fromtimestamp(1759960052.0, tz=timezone.utc).replace(tzinfo=None)
    # No location and not flagged remote: stays null rather than inventing one.
    assert onsite["location"] is None
    assert onsite["job_type"] is None
    assert onsite["tags_json"] is None
    assert onsite["published_at"] is None
    assert onsite["title"] == "Data Analyst"
    assert set(remote) == ALL_RAW_KEYS == set(onsite)


def test_jobicy_maps_its_camel_case_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_json(
        monkeypatch,
        {
            "jobs": [
                {
                    "id": 98765,
                    "url": "https://jobicy.com/jobs/98765-data-engineer",
                    "jobSlug": "98765-data-engineer",
                    "jobTitle": "Data Engineer",
                    "companyName": "Acme",
                    "jobGeo": "Anywhere",
                    "jobIndustry": ["Business Development", "Engineering"],
                    "jobType": ["Full-Time"],
                    "jobLevel": "Senior",
                    "jobExcerpt": "Short blurb",
                    "jobDescription": "<p>Long blurb</p>",
                    "pubDate": "2026-10-08 12:00:00",
                    "companyLogo": "https://jobicy.com/logo.png",
                }
            ]
        },
    )

    (posting,) = extractor.fetch_jobicy()

    assert posting["source"] == "jobicy"
    # The id arrives as a JSON number but the column is a string.
    assert posting["source_job_id"] == "98765"
    assert posting["title"] == "Data Engineer"
    assert posting["location"] == "Anywhere"
    assert posting["industry"] == "Business Development, Engineering"
    assert posting["tags_json"] == '["Business Development", "Engineering"]'
    assert posting["job_type"] == "Full-Time"
    assert posting["published_at"] == datetime(2026, 10, 8, 12, 0, 0)
    assert set(posting) == ALL_RAW_KEYS


# --- pagination ---------------------------------------------------------------


def page_server(
    monkeypatch: pytest.MonkeyPatch, pages: list[dict[str, Any]], pauses: list[float] | None = None
) -> list[str]:
    """Serve one stubbed payload per request, recording the URLs asked for."""
    requested: list[str] = []

    def fake(url: str) -> dict[str, Any]:
        requested.append(url)
        return pages[len(requested) - 1]

    monkeypatch.setattr(extractor, "_get_json", fake)
    # The pacing is part of the behaviour but its value is a tuning knob, so the
    # tests record the pauses instead of waiting them out.
    monkeypatch.setattr(extractor.time, "sleep", (pauses if pauses is not None else []).append)
    return requested


def test_arbeitnow_walks_the_next_link_and_stops_when_the_board_ends(monkeypatch: pytest.MonkeyPatch) -> None:
    """One request cannot return arbeitnow whole: it serves 300 a page and paginates."""
    second = "https://www.arbeitnow.com/api/job-board-api?page=2"
    requested = page_server(
        monkeypatch,
        [
            {"data": [{"slug": "a", "title": "Data Engineer"}], "links": {"next": second}},
            {"data": [{"slug": "b", "title": "Data Analyst"}], "links": {"next": None}},
        ],
    )

    postings = extractor.fetch_arbeitnow()

    assert [posting["source_job_id"] for posting in postings] == ["a", "b"]
    assert requested == [extractor.ARBEITNOW_URL, second]


def test_pagination_stops_at_the_page_cap_and_says_so(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """An endless board must not become an endless run, and truncation is logged."""
    pauses: list[float] = []
    requested = page_server(
        monkeypatch,
        [
            {
                "data": [{"slug": f"s{page}", "title": "Data Engineer"}],
                "links": {"next": f"https://www.arbeitnow.com/api/job-board-api?page={page + 1}"},
            }
            for page in range(1, extractor.MAX_PAGES_PER_SOURCE + 2)
        ],
        pauses,
    )

    with caplog.at_level(logging.WARNING):
        postings = extractor.fetch_arbeitnow()

    assert len(requested) == extractor.MAX_PAGES_PER_SOURCE
    assert len(postings) == extractor.MAX_PAGES_PER_SOURCE
    # Paced rather than burst, which is what arbeitnow's 429 punishes.
    assert len(pauses) == extractor.MAX_PAGES_PER_SOURCE - 1
    assert all(pause > 0 for pause in pauses)
    assert "raise MAX_PAGES_PER_SOURCE" in caplog.text


def test_jobicy_follows_its_cursor_and_stops_when_hasmore_clears(monkeypatch: pytest.MonkeyPatch) -> None:
    """jobicy caps a request at count=100 and hands the next page back as a cursor."""
    requested = page_server(
        monkeypatch,
        [
            {"jobs": [{"id": 1, "jobTitle": "Data Engineer"}], "hasMore": True, "nextCursor": "abc/def="},
            {"jobs": [{"id": 2, "jobTitle": "Data Analyst"}], "hasMore": False, "nextCursor": "xyz"},
        ],
    )

    postings = extractor.fetch_jobicy()

    assert [posting["source_job_id"] for posting in postings] == ["1", "2"]
    assert requested[0] == extractor.JOBICY_URL
    assert "cursor=abc%2Fdef%3D" in requested[1]


def test_sources_declare_the_names_dbt_accepts() -> None:
    """The extractor's source literals and the dbt accepted_values must agree.

    Two files state the same set: the `SOURCES` tuple here and the
    `accepted_values` test on `raw_job_postings.source`. If a source is added to
    one and not the other, the dbt test fails on data the extractor wrote.
    """
    spec = yaml.safe_load(SOURCE_SPEC.read_text())
    columns = spec["sources"][0]["tables"][0]["columns"]
    source_column = next(column for column in columns if column["name"] == "source")
    entry = next(test for test in source_column["data_tests"] if isinstance(test, dict) and "accepted_values" in test)
    # dbt 1.12 deprecates generic-test arguments passed as flat keys, so the
    # values sit under `arguments:` -- the same nesting the sibling project uses.
    assert "arguments" in entry["accepted_values"], f"generic-test args must nest under `arguments:`: {entry}"
    accepted = entry["accepted_values"]["arguments"]["values"]

    assert sorted(accepted) == sorted(name for name, _ in extractor.SOURCES)


# --- timestamp parsing --------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-08T21:47:32", datetime(2026, 10, 8, 21, 47, 32)),
        ("2026-10-08T21:47:32Z", datetime(2026, 10, 8, 21, 47, 32)),
        ("2026-10-08 21:47:32", datetime(2026, 10, 8, 21, 47, 32)),
        # Offsets are normalised to UTC so every source lands in one timeline.
        ("2026-10-08T23:47:32+02:00", datetime(2026, 10, 8, 21, 47, 32)),
        (1_700_000_000, datetime(2023, 11, 14, 22, 13, 20)),
    ],
)
def test_timestamps_normalise_to_naive_utc(raw: Any, expected: datetime) -> None:
    parsed = extractor._parse_timestamp(raw)

    assert parsed == expected
    assert parsed is not None and parsed.tzinfo is None  # the connector rejects tz-aware values


@pytest.mark.parametrize("raw", [None, "", "not a date", "2026-13-45"])
def test_unusable_timestamps_become_null(raw: Any) -> None:
    assert extractor._parse_timestamp(raw) is None


# --- the fetch loop -----------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (urllib.error.URLError("connection reset"), "connection reset"),
        (TimeoutError("timed out"), "timed out"),
        (ValueError("no json object"), "no json object"),
        # A payload whose shape changed fails inside the fetcher as an
        # AttributeError/KeyError, not as a URLError.
        (AttributeError("'list' object has no attribute 'get'"), "'list' object has no attribute 'get'"),
        (KeyError("jobs"), "'jobs'"),
    ],
)
def test_a_broken_source_is_skipped_and_named(
    databricks_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    message: str,
) -> None:
    def broken() -> list[dict[str, Any]]:
        raise error

    def healthy() -> list[dict[str, Any]]:
        return [raw_row(source="jobicy", source_job_id="7")]

    monkeypatch.setattr(extractor, "SOURCES", (("remotive", broken), ("jobicy", healthy)))
    connection = FakeConnection()
    monkeypatch.setattr(_common, "connect", lambda _env: connection)

    with caplog.at_level(logging.WARNING):
        written = extractor.extract_job_postings()

    assert written == 1, "the healthy source's rows still reach the warehouse"
    assert connection.closed
    assert "remotive" in caplog.text and message in caplog.text


def test_an_unknown_id_becomes_null_not_the_string_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """`str(None)` would be "None": a truthy key that passes dbt's not_null test.

    Every id-less posting would then share the key `<source>:None`, dedupe to one
    row in staging, and silently outlive the check meant to catch it. The column
    has to be null so the fetch loop can drop the posting instead.
    """
    stub_json(monkeypatch, {"jobs": [{"title": "Data Engineer"}]})
    (posting,) = extractor.fetch_remotive()
    assert posting["source_job_id"] is None

    stub_json(monkeypatch, {"jobs": [{"jobTitle": "Data Engineer"}]})
    (posting,) = extractor.fetch_jobicy()
    assert posting["source_job_id"] is None


def test_postings_without_a_source_job_id_are_dropped_and_named(
    databricks_env: dict[str, str], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Raw's not_null test on source_job_id and staging's dedupe both need the key."""

    def unkeyed() -> list[dict[str, Any]]:
        # arbeitnow builds the key from a slug the payload can omit.
        return [raw_row(source="arbeitnow", source_job_id=None), raw_row(source="arbeitnow", source_job_id="s")]

    monkeypatch.setattr(extractor, "SOURCES", (("arbeitnow", unkeyed),))
    connection = FakeConnection()
    monkeypatch.setattr(_common, "connect", lambda _env: connection)

    with caplog.at_level(logging.WARNING):
        written = extractor.extract_job_postings()

    assert written == 1, "the keyed posting still reaches the warehouse"
    assert "Skipped 1 postings from arbeitnow without a source_job_id" in caplog.text


def test_every_source_failing_aborts_before_touching_the_warehouse(
    databricks_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken() -> list[dict[str, Any]]:
        raise urllib.error.URLError("offline")

    def must_not_connect(_env: dict[str, str]) -> FakeConnection:
        raise AssertionError("connect() must not be reached when every source failed")

    monkeypatch.setattr(extractor, "SOURCES", (("remotive", broken), ("arbeitnow", broken)))
    monkeypatch.setattr(_common, "connect", must_not_connect)

    with pytest.raises(RuntimeError, match="Every source failed: remotive, arbeitnow"):
        extractor.extract_job_postings()


def test_extract_rejects_a_missing_connection_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _common.REQUIRED_ENV:
        monkeypatch.delenv(key, raising=False)
    fetched: list[str] = []

    def should_not_run() -> list[dict[str, Any]]:
        fetched.append("remotive")
        return []

    monkeypatch.setattr(extractor, "SOURCES", (("remotive", should_not_run),))

    with pytest.raises(RuntimeError, match="Missing required environment variable"):
        extractor.extract_job_postings()

    assert fetched == [], "no source is fetched before the settings are validated"


def test_placeholder_settings_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Values copied from .env.example must not pass for real settings."""
    monkeypatch.setenv("DATABRICKS_HOSTNAME", "your_workspace_host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/000")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi0123456789")

    with pytest.raises(RuntimeError, match="placeholders: DATABRICKS_HOSTNAME"):
        _common.require_env()


def test_connection_settings_do_not_include_a_token(databricks_env: dict[str, str]) -> None:
    """A token in the environment lasts an hour, and requiring one is what broke
    every orchestrated run after the code server had been up that long.

    `databricks_env` leaves `DATABRICKS_TOKEN` unset, so this pins that the settings
    still resolve — i.e. that a token is not among the required ones.
    """
    assert set(_common.require_env()) == {"DATABRICKS_HOSTNAME", "DATABRICKS_HTTP_PATH"}


def test_connect_authenticates_from_the_cli_profile(
    databricks_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """`connect()` hands the connector a credentials *factory*, never a token.

    The connector calls the factory once to obtain a header factory, then calls
    that per request — so a fixed token, the shape that produced
    `RequestError: refresh the databricks token`, would defeat the CLI profile's
    refresh. That the second call differs is the point, not a stub artefact.
    """
    minted = iter(range(1, 10))
    captured: list[dict[str, Any]] = []

    class StubConfig:
        def authenticate(self) -> dict[str, str]:
            return {"Authorization": f"Bearer fresh-{next(minted)}"}

    monkeypatch.setattr(_common, "databricks_config", StubConfig)
    monkeypatch.setattr(_common.sql, "connect", lambda **kwargs: captured.append(kwargs) or FakeConnection())

    _common.connect(_common.require_env())

    (kwargs,) = captured
    assert "access_token" not in kwargs
    assert kwargs["server_hostname"] == "example.invalid"  # the scheme was stripped
    header_factory = kwargs["credentials_provider"]()
    assert header_factory() == {"Authorization": "Bearer fresh-1"}
    assert header_factory() == {"Authorization": "Bearer fresh-2"}


def test_fetchers_only_read_declared_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every fetcher emits exactly the raw column set, so no value lands as null by typo."""
    fetchers: tuple[tuple[str, Callable[[], list[dict[str, Any]]], dict[str, Any]], ...] = (
        ("fetch_remotive", extractor.fetch_remotive, {"jobs": [{"id": 1, "title": "Data Engineer"}]}),
        ("fetch_arbeitnow", extractor.fetch_arbeitnow, {"data": [{"slug": "s", "title": "Data Engineer"}]}),
        ("fetch_jobicy", extractor.fetch_jobicy, {"jobs": [{"id": 1, "jobTitle": "Data Engineer"}]}),
    )

    for name, fetcher, payload in fetchers:
        stub_json(monkeypatch, payload)
        (posting,) = fetcher()
        missing = ALL_RAW_KEYS - set(posting)
        assert not missing, f"{name} omits {sorted(missing)}"
