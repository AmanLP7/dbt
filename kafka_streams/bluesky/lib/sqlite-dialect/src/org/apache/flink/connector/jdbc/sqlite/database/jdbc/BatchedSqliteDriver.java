/*
 * A SQLite JDBC driver that wraps every statement batch in one transaction.
 *
 * The stock `org.sqlite.JDBC` leaves connections in auto-commit mode, and
 * `Statement.executeBatch()` commits each statement separately. Each commit
 * fsyncs the WAL, which costs ~10 ms on this machine, so a sink that batches
 * 5,000 rows still pays 5,000 fsyncs: measured 100 rows/s with `executeBatch`
 * against 24,252 rows/s for the same rows committed once. Flink's JDBC sink
 * never calls `Connection.setAutoCommit(false)` and never calls `commit()` -
 * `JdbcOutputFormat.open()` only asks the connection provider for a connection
 * - so the sink cannot ask for this behaviour through configuration.
 *
 * This driver supplies it: a statement batch runs inside one transaction that is
 * committed when the batch returns. The commit therefore lands where Flink's
 * `sink.buffer-flush` boundary already is, and a failed batch is rolled back
 * instead of being half-applied. The sink stays at-least-once: a replay of the
 * same rows is absorbed by `INSERT OR IGNORE`.
 *
 * `SimpleJdbcConnectionProvider` instantiates the configured driver with
 * `Class.newInstance()`, which needs a public no-argument constructor, and then
 * calls `Driver.connect` on that instance directly rather than going through
 * `DriverManager`. So the delegate is loaded lazily here and never registered.
 */
package org.apache.flink.connector.jdbc.sqlite.database.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/** A {@link Driver} that commits once per statement batch. */
public final class BatchedSqliteDriver implements Driver {

    private static final String URL_PREFIX = "jdbc:sqlite:";
    private static final String DELEGATE = "org.sqlite.JDBC";

    private final Driver delegate;

    /** Required by {@code Class.newInstance()}, which Flink's provider uses. */
    public BatchedSqliteDriver() {
        this.delegate = loadDelegate();
    }

    private static Driver loadDelegate() {
        try {
            return (Driver) Class.forName(DELEGATE).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("SQLite JDBC driver " + DELEGATE + " is not on the classpath", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Connection connection = delegate.connect(url, info);
        if (connection == null) {
            return null;
        }
        return (Connection)
                Proxy.newProxyInstance(
                        BatchedSqliteDriver.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        new ConnectionHandler(connection));
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return delegate.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getGlobal();
    }

    /** Hands back statements that commit once per batch. */
    private static final class ConnectionHandler implements InvocationHandler {

        private final Connection target;

        private ConnectionHandler(Connection target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "equals":
                    return proxy == args[0];
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "toString":
                    return "BatchedSqliteConnection[" + target + "]";
                case "prepareStatement":
                case "prepareCall":
                case "createStatement":
                    return wrap(invokeTarget(method, args));
                default:
                    return invokeTarget(method, args);
            }
        }

        private Object wrap(Object statement) {
            Class<?> type =
                    statement instanceof CallableStatement
                            ? CallableStatement.class
                            : (statement instanceof PreparedStatement ? PreparedStatement.class : Statement.class);
            return Proxy.newProxyInstance(
                    BatchedSqliteDriver.class.getClassLoader(),
                    new Class<?>[] {type},
                    new StatementHandler((Statement) statement, target));
        }

        private Object invokeTarget(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    /** Runs the batch in one transaction, and rolls it back instead of committing on failure. */
    private static final class StatementHandler implements InvocationHandler {

        private final Statement target;
        private final Connection connection;

        private StatementHandler(Statement target, Connection connection) {
            this.target = target;
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "equals":
                    return proxy == args[0];
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "toString":
                    return "BatchedSqliteStatement[" + target + "]";
                case "executeBatch":
                case "executeLargeBatch":
                    return executeBatchInTransaction(method, args);
                default:
                    return invokeTarget(method, args);
            }
        }

        private Object executeBatchInTransaction(Method method, Object[] args) throws Throwable {
            // Only a batch is wrapped: the sink's own DDL and `PRAGMA` statements
            // must run in auto-commit mode, since SQLite refuses a journal-mode
            // change from inside a transaction.
            boolean restoreAutoCommit = connection.getAutoCommit();
            if (restoreAutoCommit) {
                connection.setAutoCommit(false);
            }
            Object result;
            try {
                result = invokeTarget(method, args);
                connection.commit();
            } catch (Throwable failure) {
                rollback();
                throw failure;
            } finally {
                if (restoreAutoCommit) {
                    connection.setAutoCommit(true);
                }
            }
            return result;
        }

        private void rollback() {
            try {
                connection.rollback();
            } catch (SQLException ignored) {
                // The original failure is the one worth reporting.
            }
        }

        private Object invokeTarget(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}
