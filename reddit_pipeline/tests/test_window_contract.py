"""Cross-artifact invariants that the type-checkers and linters cannot see."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from ingestion.reddit import _common

REPO_ROOT = Path(__file__).resolve().parents[1]


def _render_macro(name: str) -> str:
    """Render a project macro through jinja, the way dbt and sqlfluff do."""
    environment = Environment(loader=FileSystemLoader(REPO_ROOT / "macros"))
    module = environment.get_template(f"{name}.sql").module
    return getattr(module, name)()


def test_mart_window_matches_the_ingestion_window():
    """`mart_comment_history` replicates the ingestion window in SQL.

    The two definitions live in different languages, so nothing but this test
    stops them drifting apart after a change to either one.
    """
    assert int(_render_macro("window_hours")) == _common.INCREMENTAL_WINDOW_HOURS
