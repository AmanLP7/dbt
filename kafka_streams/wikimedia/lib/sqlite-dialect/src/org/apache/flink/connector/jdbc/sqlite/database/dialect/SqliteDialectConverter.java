/*
 * Binds Flink's internal `RowData` fields onto a JDBC `PreparedStatement` for
 * SQLite. The value conversion itself is inherited: `AbstractDialectConverter`
 * already maps every logical type SQLite accepts (see
 * `SqliteDialect#supportedTypes`) onto the matching `setString`/`setInt`/
 * `setLong`/... call. Only the name is SQLite-specific.
 */
package org.apache.flink.connector.jdbc.sqlite.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.types.logical.RowType;

/** Row converter for {@link SqliteDialect}. */
@Internal
public class SqliteDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    public SqliteDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "SQLite";
    }
}
