"""Offline test doubles for the extractors: no network, no warehouse."""

from __future__ import annotations

from typing import Any

from ingestion.jobs import _common  # noqa: F401  (re-exported for callers' convenience)


class FakeCursor:
    """Records every statement and its bound parameters."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, statement: str, parameters: tuple[Any, ...] = ()) -> FakeCursor:
        self.statements.append((statement, tuple(parameters)))
        return self

    def close(self) -> None:
        pass

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    @property
    def inserts(self) -> list[tuple[str, tuple[Any, ...]]]:
        """Only the `insert into ...` statements, in execution order."""
        return [(sql, params) for sql, params in self.statements if sql.lower().startswith("insert into")]

    @property
    def sql(self) -> str:
        return " | ".join(sql for sql, _ in self.statements)


class FakeConnection:
    """Context manager standing in for `databricks.sql.client.Connection`."""

    def __init__(self, cursor: FakeCursor | None = None) -> None:
        self._cursor = cursor or FakeCursor()
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *_exc: object) -> bool:
        self.close()
        return False


def raw_row(**overrides: Any) -> dict[str, Any]:
    """A complete raw row (every RAW_COLUMNS key except `ingested_at`)."""
    row: dict[str, Any] = dict.fromkeys([column for column in _common.RAW_COLUMNS if column != "ingested_at"], None)
    row.update(source="remotive", source_job_id="1", title="Data Engineer")
    row.update(overrides)
    return row
