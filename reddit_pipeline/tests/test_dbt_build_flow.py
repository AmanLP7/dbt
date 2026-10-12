"""The dbt build flow: credential hand-off, the dbt invocation, and failure modes."""

from __future__ import annotations

import types

import pytest

from flows import dbt_build_flow


@pytest.fixture(autouse=True)
def databricks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_HOSTNAME", "https://example.invalid")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/000")


def test_dbt_env_injects_a_token_from_the_cli_profile(monkeypatch):
    monkeypatch.setattr(
        dbt_build_flow,
        "Config",
        lambda **_: types.SimpleNamespace(authenticate=lambda: {"Authorization": "Bearer abc123"}),
    )

    environment = dbt_build_flow._dbt_env()

    assert environment["DATABRICKS_TOKEN"] == "abc123"
    # The extractors' own host/path travel with it, so the two halves agree.
    assert environment["DATABRICKS_HOSTNAME"] == "https://example.invalid"


def test_dbt_env_names_the_missing_variables(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOSTNAME", raising=False)
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

    with pytest.raises(RuntimeError, match="DATABRICKS_HOSTNAME"):
        dbt_build_flow._dbt_env()


def test_missing_dbt_binary_raises_a_helpful_error(monkeypatch):
    monkeypatch.setattr(dbt_build_flow.shutil, "which", lambda _: None)

    with pytest.raises(RuntimeError, match="dbt"):
        dbt_build_flow.run_dbt_build.fn()


def test_run_dbt_build_invokes_dbt_with_the_repo_profile(monkeypatch):
    monkeypatch.setattr(dbt_build_flow.shutil, "which", lambda _: "/usr/bin/dbt")
    monkeypatch.setattr(dbt_build_flow, "_dbt_env", lambda: {"DATABRICKS_TOKEN": "t"})
    seen: dict = {}
    monkeypatch.setattr(dbt_build_flow.subprocess, "run", lambda argv, **kw: seen.update(argv=argv, kw=kw))

    dbt_build_flow.run_dbt_build.fn()

    # The dbt profile lives in the repo (profiles/), not in ~/.dbt: that is what
    # makes the flow's credentials independent of the local PAT.
    assert seen["argv"] == ["/usr/bin/dbt", "build", "--profiles-dir", str(dbt_build_flow.PROFILES_DIR)]
    assert seen["kw"]["cwd"] == dbt_build_flow.PROJECT_ROOT
    assert seen["kw"]["check"] is True  # a non-zero dbt exit must fail the task


def test_dbt_build_flow_runs_the_build_task(monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(dbt_build_flow, "run_dbt_build", lambda: ran.append("build"))

    dbt_build_flow.dbt_build_flow.fn()

    assert ran == ["build"]
