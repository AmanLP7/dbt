"""Data-quality contract for the `role_keywords` seed the marts join against.

`mart_role_listings` matches listings with `title_normalized like '%' || keyword || '%'`,
so anything odd in this file silently reshapes which postings count as a role.
"""

from __future__ import annotations

import csv
from pathlib import Path

SEED = Path(__file__).resolve().parents[1] / "seeds" / "role_keywords.csv"

# `%` matches any run of characters and `_` any single one, so either would widen
# the substring match far beyond the role the keyword names.
LIKE_WILDCARDS = ("%", "_", "\\")


def load_seed() -> list[dict[str, str]]:
    with SEED.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_seed_has_the_documented_header_and_rows() -> None:
    with SEED.open(newline="", encoding="utf-8") as handle:
        header = csv.DictReader(handle).fieldnames

    assert header == ["role_family", "keyword"]
    assert load_seed(), "an empty seed would make every role mart empty"


def test_role_family_and_keyword_are_always_populated() -> None:
    for row in load_seed():
        # A blank keyword would compile to `like '%%'` and match every listing.
        assert row["keyword"].strip(), f"blank keyword for {row['role_family']!r}"
        assert row["role_family"].strip(), f"blank role family for {row!r}"


def test_keywords_contain_no_like_wildcards() -> None:
    offenders = [
        (row["role_family"], row["keyword"])
        for row in load_seed()
        if any(char in row["keyword"] for char in LIKE_WILDCARDS)
    ]

    assert not offenders, f"LIKE wildcard in keyword: {offenders}"


def test_role_family_and_keyword_pairs_are_unique() -> None:
    pairs = [(row["role_family"], row["keyword"]) for row in load_seed()]

    # A repeated pair adds no reach to the join; `select distinct` in the mart is
    # the only reason it would not inflate the output.
    assert len(set(pairs)) == len(pairs)
