"""Offline test doubles for the loader: no SQLite file on disk, no warehouse."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.bluesky import _common

# A fixed event time and the two sizes the batching tests need.
EVENT_DT = "2026-10-28T15:31:52.763977Z"
INGESTED_AT = "2026-10-28T15:31:53.431+00:00"
SMALL_PAYLOAD = "x" * 200


def event_row(
    offset: int,
    *,
    partition: int = 0,
    payload: str = SMALL_PAYLOAD,
    seq: int | None = None,
    topic: str = _common.TOPIC,
) -> tuple[Any, ...]:
    """One `main.events` row, in the column order of `RAW_COLUMNS`.

    `seq` is SQLite's primary key, so it is unique across partitions too; the
    default derives it from both, as Jetstream's own counter would.
    """
    return (
        f"at://did:plc:test/app.bsky.feed.post/{partition}-{offset}",
        INGESTED_AT,
        EVENT_DT,
        "commit",
        "app.bsky.feed.post",
        "did:plc:test",
        "create",
        f"{offset}",
        seq if seq is not None else 26_000_000_000 + partition * 1_000_000 + offset,
        topic,
        partition,
        offset,
        payload,
    )


class FakeCursor:
    """Records every statement; answers the loader's read queries from canned rows."""

    def __init__(self, watermarks: dict[int, int] | None = None) -> None:
        self.statements: list[tuple[str, Any]] = []
        self._watermarks = watermarks or {}
        self.closed = False

    def execute(self, operation: str, parameters: Any = None) -> FakeCursor:
        self.statements.append((operation, parameters))
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        statement = self.statements[-1][0]
        if "MAX(`offset`)" in statement:
            return list(self._watermarks.items())
        return []

    def close(self) -> None:
        self.closed = True

    @property
    def inserts(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [statement for statement in self.statements if statement[0].startswith("INSERT")]

    @property
    def statement_text(self) -> str:
        return " ".join(statement for statement, _ in self.statements)

    def inserted(self) -> list[tuple[Any, ...]]:
        """Regroup every bound INSERT parameter back into rows."""
        width = len(_common.RAW_COLUMNS)
        params = [value for _, bound in self.inserts for value in (bound or ())]
        return [tuple(params[index : index + width]) for index in range(0, len(params), width)]


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class FakeWarehouse:
    """A stand-in for `_common.connect`: one cursor, and a configurable watermark."""

    def __init__(self, watermarks: dict[int, int] | None = None) -> None:
        self.cursor_obj = FakeCursor(watermarks)
        self.connection = FakeConnection(self.cursor_obj)
        self.connects = 0

    def connect(self, env: dict[str, str]) -> tuple[FakeConnection, FakeCursor]:
        self.connects += 1
        return self.connection, self.cursor_obj


class FakeVolumeFiles:
    """A recording stand-in for `_common.VolumeFiles`.

    The real class talks to the Files API, which cannot be reached offline and
    cannot be faked at the SDK boundary without reimplementing it. This keeps the
    same three methods and records what the loader asked for, so the tests can
    assert on the volume's state rather than on SDK call syntax.

    Two quirks of the real API are reproduced on purpose, because both broke this
    loader once and neither is visible in a green build:

    - `list_dirs` returns directory paths **with a trailing slash**, so a prefix
      match that takes the last slash-separated segment finds nothing.
    - `remove_dir` has to delete every file before the directory will go, because
      the API's `delete_directory` refuses a non-empty one.
    """

    def __init__(self, dirs: list[str] | None = None, files: list[str] | None = None) -> None:
        self.uploads: list[tuple[str, int]] = []
        self.removed: list[str] = []
        self._dirs = [path if path.endswith("/") else path + "/" for path in (dirs or [])]
        self._files = list(files or [])

    def upload(self, remote_path: str, local_path: Path) -> None:
        self.uploads.append((remote_path, local_path.stat().st_size))
        self._files.append(remote_path)

    def list_dirs(self, remote_dir: str, *, prefix: str = "") -> list[str]:
        return sorted(path for path in self._dirs if path.rstrip("/").rsplit("/", 1)[-1].startswith(prefix))

    def remove_dir(self, remote_dir: str) -> int:
        self.removed.append(remote_dir)
        self._dirs = [path for path in self._dirs if path != remote_dir]
        stale = [path for path in self._files if path.startswith(remote_dir.rstrip("/") + "/")]
        self._files = [path for path in self._files if not path.startswith(remote_dir.rstrip("/") + "/")]
        return len(stale)

    @property
    def uploaded_paths(self) -> list[str]:
        return [path for path, _ in self.uploads]

    def listing(self, remote_dir: str) -> list[str]:
        """The files currently under a directory, for asserting on what survived."""
        return sorted(path for path in self._files if path.startswith(remote_dir + "/"))
