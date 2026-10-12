---
name: lint-and-typecheck
description: Lint, format, type-check and test this project. Covers sqlfluff (Databricks dialect, jinja templater, no dbt install or warehouse connection required), ruff, pyright, pytest, and the offline `dbt parse` check; the deliberate rule deviations that look like misconfiguration, why pyright reports no findings, the two CI jobs, and the uv-tool/PATH trap that shadows these pip installs.
globs: ["**/*.sql", "**/*.py", "pyproject.toml", ".sqlfluff", ".github/workflows/*.yml", "requirements*.txt"]
---

# Lint, format, type-check, test

## Commands

```bash
sqlfluff lint                                   # report SQL violations (models/ and macros/)
sqlfluff fix                                    # apply SQL auto-fixes
ruff check .                                    # report Python violations
ruff check --fix .                              # apply safe Python auto-fixes
ruff format .                                   # format Python and the Python fences in *.md (authoritative)
pyright                                         # type check Python
pytest                                          # offline extractor/flow/window tests
DBT_PROFILES_DIR=profiles dbt parse             # parse models/yml/properties; no warehouse connection
```

Expected clean output: `All Finished!` (sqlfluff), `All checks passed!` (ruff),
`20 files already formatted` (ruff format), `N passed` (pytest), and pyright's
`0 errors, 0 warnings, 0 informations` — it is clean. Pyright used to be
false-positive-prone: the `bad_vars` comprehension in
`ingestion/reddit/_common.py` called `os.getenv(var)` twice, which pyright widened
to `str | None` and flagged under `reportOperatorIssue`. The walrus form
`not (value := os.getenv(var)) or "your_" in value` binds the call to `value`,
which pyright narrows to `str` in the `"your_" in value` operand, so the finding
is gone. That clean run assumes the runtime deps from `requirements.txt` are
installed; on a checkout without them, pyright adds one `reportMissingImports`
per uninstalled import.

## What `ruff format` covers

**`ruff format` covers Markdown; `ruff check` does not.** ruff formats Python code
fences inside `.md` files, so `ruff format --check .` covers **20 files**: the 16
Python sources — 6 under `ingestion/` (`ingestion/__init__.py`,
`ingestion/reddit/__init__.py`, `ingestion/reddit/_common.py`, and the three
extractors), 3 under `flows/` (`flows/__init__.py`, `flows/dbt_build_flow.py`,
`flows/reddit_extract_flow.py`) and 7 under `tests/` (`tests/__init__.py`,
`tests/conftest.py`, `tests/fakes.py`, `tests/test_dbt_build_flow.py`,
`tests/test_extractors.py`, `tests/test_flow.py`, `tests/test_window_contract.py`)
— plus `README.md` and the 3 `.agents/skills/*/SKILL.md` files. Editing a doc can
therefore turn `ruff format --check .` red. `ruff check` is narrower: given a `.md`
path it reports `No Python files found under the given path(s)`. After editing
docs, run `ruff format .` to keep the check green.

Every ```python fence in these docs must be ruff-clean: line length 120, isort
order, no trailing whitespace.

## Config

| File | Owns |
| --- | --- |
| `.sqlfluff` | dialect, templater, `ignore_paths`, all rule tuning |
| `pyproject.toml` | `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.pyright]` |
| `requirements-dev.txt` | pinned versions: sqlfluff 4.4.0, ruff 0.17.0, pyright 1.1.414, debugpy 1.8.22, pytest 8.4.2 |
| `requirements.txt` | pinned runtime deps: prefect 3.8.8, praw 8.0.3, databricks-sql-connector 4.4.0, databricks-sdk 0.117.0, python-dotenv 1.2.4, dbt-databricks 1.12.6 |

Install into the **active** Python environment: `pip install -r requirements-dev.txt`
and, for running the pipeline itself, `pip install -r requirements.txt`.

## Non-obvious facts

**sqlfluff runs without dbt or a warehouse.** `.sqlfluff` sets `templater = jinja`
plus `apply_dbt_builtins = True`, so `{{ ref() }}` and `{{ source() }}` resolve as
stubs. Do not switch to `templater = dbt`: it requires `dbt-databricks` importable
inside sqlfluff's own environment, and `dbt` is not installed there (`command -v
dbt` finds nothing; the pyenv 3.12.2 interpreter has no `dbt` module). Dialect is
`databricks` (inherits `sparksql`). `load_macros_from_path = macros` resolves the
project macros (`add_audit_fields()`, `window_hours()`) the same way dbt does; note
that `apply_dbt_builtins` stubs `var()` with a placeholder *token*, which is exactly
why the rolling-window constant is a macro rather than a dbt var — a `var()` inside
`interval … hours` renders unparsable.

**Rule deviations are intentional, not drift.**

- `aliasing.table = implicit` — all 12 aliased table references across the 5
  non-staging models use implicit aliases (`from posts p`). Flipping to the
  default `explicit` means editing those 5 models.
- `references.keywords` ignores `source, users, title`. None are reserved in the
  Databricks dialect; `title` is a raw column of `r_worldnews` that propagates
  into marts, so renaming it would change the output schema.
- Capitalisation rules are pinned to lower case to match the existing models.

**pyright is expected to report 0 findings.** `_common.py`'s `bad_vars`
comprehension uses a walrus:

```python
bad_vars = [var for var in REQUIRED_ENV if not (value := os.getenv(var)) or "your_" in value]
```

Binding `os.getenv(var)` to `value` narrows `value` to `str` in the `"your_" in
value` operand, so `reportOperatorIssue` no longer fires; the previous double
`os.getenv(var)` call stayed `str | None` and tripped it. Runtime behaviour is
unchanged: an unset var is flagged, a value containing `your_` is flagged, and a
real value is not (verified).

**`reportMissingImports` is an environment signal, not a code defect.** Only the
extraction runtimes are absent: `praw`, `databricks-sql-connector` (imported as
`databricks.sql`) and `databricks-sdk` (imported as `databricks.sdk.core`), all
imported at the top of `ingestion/reddit/_common.py`. Installing them clears the
findings; the rule is deliberately not disabled so an incomplete environment stays
visible. There is no per-import count to quote — it is simply one finding per
unresolved import.

`python-dotenv` is already importable here because Prefect depends on it, so the
`import dotenv` line resolves. Do not "fix" that asymmetry by pinning or
uninstalling dotenv: the scripts' `.env` handling is a genuine runtime dependency
and the package is pinned in `requirements.txt`.

**`dbt parse` needs no warehouse.** CI runs it with dummy `DATABRICKS_*` values,
because `profiles/profiles.yml` only reads them through `env_var(...)` and parsing
never opens a connection. It catches invalid model/yml/property changes the
linters cannot see. Run it by hand with the profile's env vars exported (the
repo-owned profile reads them through `env_var(...)`, with no defaults):

```bash
DATABRICKS_HOSTNAME=... DATABRICKS_HTTP_PATH=... DATABRICKS_TOKEN=... \
    DBT_PROFILES_DIR=profiles dbt parse
```

**CI has two jobs.** `.github/workflows/ci.yml` triggers on push to `main` and on
every pull request:

- `lint`, on `ubuntu-latest` with Python 3.10, installs both requirements files
  and then runs `ruff format --check .`, `ruff check .`, `sqlfluff lint`,
  `pyright`, `dbt parse --profiles-dir profiles` (with dummy `DATABRICKS_*` env)
  and `pytest`, failing on any non-zero exit.
- `workflow-lint` runs `docker://rhysd/actionlint:1.7.7` to lint the workflow
  files themselves (syntax, expression contexts, action inputs/outputs).

**pytest is offline.** `tests/` holds the suite; `conftest.py` patches the fakes
into `ingestion.reddit._common` (`praw.Reddit`, `sql.connect`, `time`, `Config`)
and neutralises `load_dotenv`, so no network and no warehouse are needed. pyright
covers `tests/` too, so keep new fixtures typed.

**Never install these tools with `uv tool`.** `~/.local/bin` precedes the pyenv
shims on `PATH` in this shell, so a uv-tool copy of `ruff`/`sqlfluff` shadows the
pip-installed one and silently re-introduces version skew. (An earlier
`uv tool install sqlfluff ruff` was uninstalled for exactly this reason.)

## Verifying the linters actually honour the config

Green output can also mean "nothing was linted". Prove the config is live:

```bash
mkdir -p target && printf 'SELECT 1 AS x\nFROM t\n' > target/ignore_probe.sql
sqlfluff lint .                                              # exit 0: target/ excluded
sqlfluff lint target/ignore_probe.sql --disregard-sqlfluffignores   # CP01 x3: rule set works
rm -rf target
```

`pyright --verbose` prints `Loading pyproject.toml file at …` when it picks up the
config; move a deliberately broken `.py` into `logs/` to confirm `exclude` works.
