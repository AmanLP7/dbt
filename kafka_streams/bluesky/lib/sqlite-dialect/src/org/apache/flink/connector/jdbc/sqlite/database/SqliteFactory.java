/*
 * Registers {@link SqliteDialect} with Flink's JDBC connector.
 *
 * `JdbcFactoryLoader` discovers implementations of this interface through
 * `java.util.ServiceLoader`, so this class is only reachable because
 * `META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory`
 * in this jar names it.
 */
package org.apache.flink.connector.jdbc.sqlite.database;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.sqlite.database.dialect.SqliteDialect;

/** Factory for {@link SqliteDialect}. */
@Internal
public class SqliteFactory implements JdbcFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:sqlite:");
    }

    @Override
    public JdbcDialect createDialect() {
        return new SqliteDialect();
    }

    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        throw new UnsupportedOperationException("Catalog for SQLite is not supported.");
    }
}
