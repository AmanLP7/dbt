# RULES

Hard invariants for this checkout. Procedure and reasoning live in `AGENTS.md`.

- Ad-hoc Databricks queries go through the MCP server `databricks`. The CLI and `databricks-sql-connector` are fallback only.
- A SQL-level `FAILED` (`BAD_REQUEST` with `TABLE_OR_VIEW_NOT_FOUND`, `PARSE_SYNTAX_ERROR`, `UNRESOLVED_COLUMN`, `UNKNOWN_FUNCTION`, `PERMISSION_DENIED`) means the query is wrong. Fix and retry on MCP; never re-run that text on the CLI. `[OP_NOT_SUPPORTED_READ_ONLY]` means the statement was a write: stop, never re-run it on the CLI.
- HTTP 403 is a denial to report, not a fallback trigger. A 401 means the session's cached MCP token expired: report it and restart the session, never query through the CLI to work around it.
- Fallbacks are read-only (`SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`). Never run MCP and a fallback in parallel to compare. The one non-error reason to fall back: determining what the fallback identity itself may do — MCP runs as a service principal and cannot answer that.
- Never write to a relation `dbt build` produces. A write of any kind needs an explicit yes for that exact statement from the user.
- `databricks auth token --profile DEFAULT` is always allowed; `.omp/mcp.json` needs it.
