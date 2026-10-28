"""Offline test doubles for the extractors: no network, no warehouse."""

from __future__ import annotations

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
    def deletes(self) -> list[tuple[str, Any]]:
        return [statement for statement in self.statements if statement[0].startswith("DELETE")]

    @property
    def inserts(self) -> list[tuple[str, Any]]:
        return [statement for statement in self.statements if statement[0].startswith("INSERT")]

    def inserted(self, columns: dict[str, str] | list[str]) -> list[tuple[Any, ...]]:
        """Regroup the bound INSERT parameters back into rows of `columns` width."""
        width = len(columns)
        params = [value for _, bound in self.inserts for value in (bound or ())]
        return [tuple(params[index : index + width]) for index in range(0, len(params), width)]

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
    ) -> None:
        self.id = submission_id
        self.title = title
        self.score = score
        self.url = url
        self.num_comments = num_comments
        self.created_utc = created_utc
        self.selftext = selftext
        self.author = author
        self.subreddit = "worldnews"
        self.comments = FakeCommentForest(comments or [])


class FakeSubreddit:
    def __init__(self, hot: list[FakeSubmission], new: list[FakeSubmission] | None = None) -> None:
        self._hot = hot
        self._new = new or []

    def hot(self, limit: int | None = None) -> list[FakeSubmission]:
        return self._hot if limit is None else self._hot[:limit]

    def new(self, limit: int | None = None) -> list[FakeSubmission]:
        return self._new if limit is None else self._new[:limit]


class FakeReddit:
    def __init__(self, subreddit: FakeSubreddit, name: str = "worldnews") -> None:
        self._subreddit = subreddit
        self._name = name

    def subreddit(self, name: str) -> FakeSubreddit:
        if name != self._name:
            raise KeyError(f"fake serves only r/{self._name}, not r/{name}")
        return self._subreddit
