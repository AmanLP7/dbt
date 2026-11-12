# RULES

Hard invariants for this checkout. Procedure and reasoning live in `AGENTS.md`.

- Ad-hoc Databricks queries go through the MCP server `databricks` (`system.ai.dbsql`, tools `execute_sql` and `poll_sql_result`). The CLI and `databricks-sql-connector` are fallback only.
- A SQL-level `FAILED` (`BAD_REQUEST` with `TABLE_OR_VIEW_NOT_FOUND`, `PARSE_SYNTAX_ERROR`, `UNRESOLVED_COLUMN`, `UNKNOWN_FUNCTION`, `PERMISSION_DENIED`) means the query is wrong. Fix and retry on MCP; never re-run that text on the CLI. `[OP_NOT_SUPPORTED_READ_ONLY]` means the statement was a write: stop, never re-run it on the CLI.
- A write attempted on MCP comes back as **HTTP 200** with `status.state: FAILED` and `PERMISSION_DENIED` in the body, `isError: false`. Read the body, not the status code. It is a denial, not a failure to route around.
- HTTP 403 is a denial to report, not a fallback trigger. A 401 means the MCP credential path is broken — `~/.config/omp/dbsql-token.py` must print a `Bearer` header; if it cannot, fix the credential, do not query through the CLI to work around it. The MCP entry is a stdio relay (`~/.config/omp/dbsql-mcp-bridge.py`) that holds the token until 5 minutes before its `exp`, then re-mints, and forces a fresh mint plus one retry on a 401, so an hour-old session does not lose SQL; if 401s persist through a live relay, the credential or the config is wrong, and `/mcp reload` is what re-reads `mcp.json`.
- Fallbacks are read-only (`SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`). Never run MCP and a fallback in parallel to compare. The one non-error reason to fall back: determining what the fallback identity itself may do — MCP runs as the service principal `omp-dbsql-ro` (SELECT only) and cannot answer that.
- Never write to a relation `dbt build` produces. A write of any kind needs an explicit yes for that exact statement from the user.
- `databricks auth token --profile DEFAULT` is always allowed; the fallback path needs it. The MCP path uses the service-principal token instead and never reads that CLI token.
