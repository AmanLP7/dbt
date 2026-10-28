/*
 * SQLite dialect for Flink's JDBC connector.
 *
 * Apache Flink 2.2 ships dialects for Derby, MySQL, PostgreSQL, Oracle, Db2,
 * SQL Server, CrateDB, OceanBase and Trino -- but not for SQLite, and the
 * `flink-connector-jdbc-core` artifact has no SQLite implementation. This class
 * is the missing piece: it lets the stock `connector = 'jdbc'` table sink write
 * into a SQLite file.
 *
 * Only the write path is implemented. Nothing in this flow reads SQLite through
 * Flink, so `getSelectFromStatement` inherited from `AbstractDialect` is unused.
 */
package org.apache.flink.connector.jdbc.sqlite.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** JDBC dialect for SQLite. */
@Internal
public class SqliteDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new SqliteDialectConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.sqlite.JDBC");
    }

    /**
     * SQLite folds unquoted identifiers case-insensitively, and `offset` is a
     * keyword, so every identifier is double-quoted. A literal quote inside an
     * identifier is escaped by doubling it, per SQLite's rules.
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String dialectName() {
        return "SQLite";
    }

    @Override
    public String getLimitClause(long limit) {
        return " LIMIT " + limit;
    }

    /**
     * No upsert: the sink is append-only, so there is nothing to merge - a
     * duplicate must vanish, not overwrite.
     *
     * Returning empty makes Flink generate plain inserts, which
     * {@link #getInsertIntoStatement} then turns into `INSERT OR IGNORE`. The
     * sink runs at-least-once (a restart replays the messages between the last
     * flush and the crash), so a duplicate `event_id` must be a no-op rather
     * than a job-killing constraint violation.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        // Named placeholders, not `?`: the sink binds through
        // `FieldNamedPreparedStatement`, which parses `:name` and rejects a
        // statement containing a bare `?`.
        String placeholders = Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT OR IGNORE INTO "
                + quoteIdentifier(tableName)
                + " ("
                + columns
                + ") VALUES ("
                + placeholders
                + ")";
    }

    /**
     * The types `main.events` and `main.kafka_offsets` use: SQLite stores text,
     * integers, reals and blobs, so BOOLEAN/DECIMAL/DATE are rejected rather
     * than silently bound through a converter that would lose their meaning.
     */
    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.VARBINARY);
    }
}
