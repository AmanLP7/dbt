#!/usr/bin/env bash
# Build `flink-connector-sqlite-4.1.0-2.2.jar` from `sqlite-dialect/src/`.
#
# Flink 2.2's JDBC connector ships dialects for Derby, MySQL, PostgreSQL,
# Oracle, Db2, SQL Server, CrateDB, OceanBase and Trino. SQLite is not one of
# them, and `flink-connector-jdbc-core` contains no SQLite code, so the dialect
# in this directory is what makes `'connector' = 'jdbc'` able to open a SQLite
# file. `SqliteFactory` is registered through `META-INF/services`, which is how
# `JdbcFactoryLoader` discovers it.
#
# The classpath is the connector jar plus the two Flink jars that ship inside
# PyFlink's `lib/`; no Maven build is involved.
set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYFLINK_LIB="${PYFLINK_LIB:-$LIB_DIR/../../.flink-venv/lib/python3.12/site-packages/pyflink/lib}"
OUT="$LIB_DIR/flink-connector-sqlite-4.1.0-2.2.jar"

if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/javac" ]]; then
  JAVAC="$JAVA_HOME/bin/javac"
  JAR="$JAVA_HOME/bin/jar"
else
  JAVAC="$(command -v javac || true)"
  JAR="$(command -v jar || true)"
fi
if [[ -z "$JAVAC" || -z "$JAR" ]]; then
  echo "javac and jar are required (a JDK 17, not a JRE); set JAVA_HOME." >&2
  exit 1
fi
for dep in flink-dist-2.2.0.jar flink-table-api-java-uber-2.2.0.jar; do
  if [[ ! -f "$PYFLINK_LIB/$dep" ]]; then
    echo "missing $PYFLINK_LIB/$dep; set PYFLINK_LIB to PyFlink's lib directory." >&2
    exit 1
  fi
done

BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

"$JAVAC" -nowarn \
  -cp "$LIB_DIR/flink-connector-jdbc-core-4.1.0-2.2.jar:$PYFLINK_LIB/flink-dist-2.2.0.jar:$PYFLINK_LIB/flink-table-api-java-uber-2.2.0.jar" \
  -d "$BUILD_DIR" \
  $(find "$LIB_DIR/sqlite-dialect/src" -name '*.java')

cp -r "$LIB_DIR/sqlite-dialect/src/META-INF" "$BUILD_DIR/"
"$JAR" cf "$OUT" -C "$BUILD_DIR" .
echo "built: $OUT"
