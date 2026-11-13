"""Offline test doubles for the extractors and the COPY INTO loader.

No network, no warehouse: the extractors write to a real SQLite dump, and only the
`copy_into_raw` loader talks to Databricks, through a recording `FakeCursor` and a
recording `FakeVolumeFiles`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.reddit import _common  # noqa: F401  (re-exported for callers' convenience)

# Fixed clock: every test pins `_common`'s time.time() to this epoch second.
NOW = 1_700_000_000.0
WINDOW = _common.INCREMENTAL_WINDOW_HOURS

ALL_COLUMNS = sorted(set(_common.POSTS_COLUMNS) | set(_common.COMMENTS_COLUMNS) | set(_common.USERS_COLUMNS))


class FakeCursor:
    """Records every statement; `fetchall` answers `DESCRIBE TABLE`."""

    def __init__(self, described_columns: list[str] | None = None) -> None:
        self.statements: list[tuple[str, Any]] = []
        self._described = ALL_COLUMNS if described_columns is None else described_columns
        self.closed = False

    def execute(self, operation: str, parameters: Any = None) -> FakeCursor:
        self.statements.append((operation, parameters))
        return self

    def fetchall(self) -> list[tuple[str]]:
        return [(name,) for name in self._described]

    def close(self) -> None:
        self.closed = True

    @property
    def alters(self) -> list[str]:
        return [sql for sql, _ in self.statements if sql.startswith("ALTER")]

    @property
    def sql_text(self) -> str:
        return " ".join(sql for sql, _ in self.statements)


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class FakeVolumeFiles(_common.VolumeFiles):
    """Records the Files-API calls the COPY INTO loader makes.

    `list_dirs` answers from a configurable directory tree so a test can plant a
    stale staging directory and assert it gets cleared; `upload` and `remove_dir`
    record their arguments for the same assertions.

    Subclasses `_common.VolumeFiles` so it slots into `copy_into_raw`'s
    `volume_files` parameter; `super().__init__()` is deliberately not called
    because it constructs a real `WorkspaceClient`, and every public method is
    overridden here.
    """

    def __init__(self, dirs: list[str] | None = None) -> None:
        self.uploads: list[tuple[str, str]] = []  # (remote_path, local_path)
        self.removed: list[str] = []
        self.list_calls: list[tuple[str, str]] = []  # (remote_dir, prefix)
        self._dirs = list(dirs or [])
        self._client = None  # unused: every public method below is overridden

    def upload(self, remote_path: str, local_path: Path) -> None:
        self.uploads.append((remote_path, str(local_path)))

    def list_dirs(self, remote_dir: str, *, prefix: str = "") -> list[str]:
        self.list_calls.append((remote_dir, prefix))
        return [d for d in self._dirs if d.rsplit("/", 1)[-1].startswith(prefix)]

    def remove_dir(self, remote_dir: str) -> int:
        self.removed.append(remote_dir)
        return 0


class FakeRedditor:
    def __init__(self, name: str, **fields: object) -> None:
        self.name = name
        self.id = fields.pop("id", f"t2_{name}")
        self.created_utc = fields.pop("created_utc", NOW - 1000)
        self.link_karma = fields.pop("link_karma", 1)
        self.comment_karma = fields.pop("comment_karma", 1)
        self.is_employee = fields.pop("is_employee", False)
        self.is_gold = fields.pop("is_gold", False)

    def __str__(self) -> str:
        return self.name


class BrokenRedditor:
    """A suspended/private account: `.name` resolves, any other field read raises."""

    name = "ghost"

    def __getattr__(self, item: str) -> object:
        raise RuntimeError(f"account {item} unavailable")


class FakeComment:
    def __init__(
        self,
        comment_id: str,
        post_id: str,
        body: str,
        score: int,
        author: object,
        created_utc: float,
        parent_id: str = "t3_parent",
    ) -> None:
        self.id = comment_id
        self.post_id = post_id
        self.body = body
        self.score = score
        self.author = author
        self.created_utc = created_utc
        self.parent_id = parent_id


class FakeCommentForest(list):
    def __init__(self, comments: list[FakeComment] | None = None) -> None:
        super().__init__(comments or [])
        self.last_replace_more_limit: int | None = None

    def replace_more(self, limit: int = 0) -> None:
        # The fake forest has no "more" stubs to expand; record the call instead.
        self.last_replace_more_limit = limit


class FakeSubmission:
    def __init__(
        self,
        submission_id: str,
        *,
        title: str = "a title",
        score: int = 10,
        url: str = "https://example.invalid/post",
        num_comments: int = 3,
        created_utc: float = NOW - 60,
        selftext: str = "",
        author: object = None,
        comments: list[FakeComment] | None = None,
        subreddit: str = "worldnews",
    ) -> None:
        self.id = submission_id
        self.title = title
        self.score = score
        self.url = url
        self.num_comments = num_comments
        self.created_utc = created_utc
        self.selftext = selftext
        self.author = author
        self.subreddit = subreddit
        self.comments = FakeCommentForest(comments or [])


class FakeSubreddit:
    """One subreddit's two listings; whichever listing is not given is empty."""

    def __init__(
        self,
        new: list[FakeSubmission],
        top: list[FakeSubmission] | None = None,
        name: str = "worldnews",
    ) -> None:
        self._new = new
        self._top = top or []
        self.name = name
        self.top_time_filter: str | None = None

    def new(self, limit: int | None = None) -> list[FakeSubmission]:
        return self._new if limit is None else self._new[:limit]

    def top(self, limit: int | None = None, time_filter: str | None = None) -> list[FakeSubmission]:
        self.top_time_filter = time_filter
        return self._top if limit is None else self._top[:limit]


class FakeReddit:
    """Serves named subreddits; every other name in the seed is an empty subreddit.

    The extractors read the seeded list, which is many subreddits wide, so a fake
    that served one name would turn every other name into a logged failure and
    bury the behaviour under test in warnings.
    """

    def __init__(self, subreddit: FakeSubreddit, name: str = "worldnews", unreadable: tuple[str, ...] = ()) -> None:
        self._by_name = {name: subreddit}
        self._unreadable = set(unreadable)
        self.requested: list[str] = []

    def subreddit(self, name: str) -> FakeSubreddit:
        self.requested.append(name)
        if name in self._unreadable:
            raise KeyError(f"fake cannot read r/{name}")
        return self._by_name.get(name, FakeSubreddit([], name=name))

    def served(self, name: str) -> FakeSubreddit:
        """The fake behind `name`, so a test can assert how it was asked for listings."""
        return self._by_name[name]
