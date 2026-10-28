#!/usr/bin/env bash
# Download the three Maven connector jars the Flink job needs.
#
# They are not committed: `lib/.gitignore` excludes `*.jar`, and these are large
# binaries with pinned versions that belong in a build step, not in git history.
# The fourth jar the job loads is built locally by `build-sqlite-dialect.sh`.
set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_VERSION="2.2"
MAVEN="https://repo1.maven.org/maven2"

# artifact path, file name
ARTIFACTS=(
  "org/apache/flink/flink-sql-connector-kafka/5.0.0-${FLINK_VERSION}/flink-sql-connector-kafka-5.0.0-${FLINK_VERSION}.jar"
  "org/apache/flink/flink-connector-jdbc-core/4.1.0-${FLINK_VERSION}/flink-connector-jdbc-core-4.1.0-${FLINK_VERSION}.jar"
  "org/xerial/sqlite-jdbc/3.53.4.0/sqlite-jdbc-3.53.4.0.jar"
)

for artifact in "${ARTIFACTS[@]}"; do
  name="$(basename "$artifact")"
  if [[ -f "$LIB_DIR/$name" ]]; then
    echo "have    $name"
    continue
  fi
  echo "fetch   $name"
  # Download beside the target and rename only on success: a curl that dies
  # mid-transfer would otherwise leave a truncated jar that the `-f` test above
  # accepts on every later run, and the failure would surface as a JVM-side
  # ZipException from the connector instead of here.
  curl -fsSL "$MAVEN/$artifact" -o "$LIB_DIR/$name.part"
  mv "$LIB_DIR/$name.part" "$LIB_DIR/$name"
done

echo "done: $LIB_DIR"
