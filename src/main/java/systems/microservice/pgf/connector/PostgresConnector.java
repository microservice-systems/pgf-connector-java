/*
 * Copyright (C) 2019 Microservice Systems, Inc.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package systems.microservice.pgf.connector;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.postgresql.Driver;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class PostgresConnector implements DataSource {
    private final String id;
    private final UUID uuid;
    private final Driver driver;
    private final String url;
    private final Properties properties;
    private final int size;
    private final boolean readOnly;
    private final long idle;
    private final AtomicLong touchTime;
    private final AtomicBoolean closed;
    private final Logger logger;
    private final Counter errorCounter;
    private final Counter exceptionCounter;
    private final Timer connectorTimer;
    private final Timer connectionTimer;
    private final Timer transactionTimer;
    private final Timer statementTimer;
    private final Timer preparedStatementTimer;
    private final Timer callableStatementTimer;
    private final Timer resultSetTimer;
    private final ThreadLocal<PostgresConnection> threadConnection;
    private final ConnectionFactory[] connectionFactories;

    public PostgresConnector(String id, String url, Properties properties, int size, boolean readOnly, long idle) {
        this.id = id;
        this.uuid = UUID.randomUUID();
        this.driver = new Driver();
        this.url = url;
        this.properties = properties;
        this.size = size;
        this.readOnly = readOnly;
        this.idle = idle;
        this.touchTime = new AtomicLong(System.currentTimeMillis());
        this.closed = new AtomicBoolean(false);
        this.logger = LoggerFactory.getLogger(String.format("%s.%s", PostgresConnector.class.getName(), id));
        this.errorCounter = Metrics.counter(String.format("%s.%s.errorCounter", PostgresConnector.class.getName(), id));
        this.exceptionCounter = Metrics.counter(String.format("%s.%s.exceptionCounter", PostgresConnector.class.getName(), id));
        this.connectorTimer = Metrics.timer(String.format("%s.%s.connectorTimer", PostgresConnector.class.getName(), id));
        this.connectionTimer = Metrics.timer(String.format("%s.%s.connectionTimer", PostgresConnector.class.getName(), id));
        this.transactionTimer = Metrics.timer(String.format("%s.%s.transactionTimer", PostgresConnector.class.getName(), id));
        this.statementTimer = Metrics.timer(String.format("%s.%s.statementTimer", PostgresConnector.class.getName(), id));
        this.preparedStatementTimer = Metrics.timer(String.format("%s.%s.preparedStatementTimer", PostgresConnector.class.getName(), id));
        this.callableStatementTimer = Metrics.timer(String.format("%s.%s.callableStatementTimer", PostgresConnector.class.getName(), id));
        this.resultSetTimer = Metrics.timer(String.format("%s.%s.resultSetTimer", PostgresConnector.class.getName(), id));
        this.threadConnection = new ThreadLocal<>();
        this.connectionFactories = new ConnectionFactory[size];
    }

    public String getId() {
        return id;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Driver getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public Properties getProperties() {
        return properties;
    }

    public int getSize() {
        return size;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public long getIdle() {
        return idle;
    }

    public long getTouchTime() {
        return touchTime.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public Logger getLogger() {
        return logger;
    }

    public Counter getErrorCounter() {
        return errorCounter;
    }

    public Counter getExceptionCounter() {
        return exceptionCounter;
    }

    public Timer getConnectorTimer() {
        return connectorTimer;
    }

    public Timer getConnectionTimer() {
        return connectionTimer;
    }

    public Timer getTransactionTimer() {
        return transactionTimer;
    }

    public Timer getStatementTimer() {
        return statementTimer;
    }

    public Timer getPreparedStatementTimer() {
        return preparedStatementTimer;
    }

    public Timer getCallableStatementTimer() {
        return callableStatementTimer;
    }

    public Timer getResultSetTimer() {
        return resultSetTimer;
    }

    @Override
    public PostgresConnection getConnection() throws SQLException {
        return null;
    }

    @Override
    public PostgresConnection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    private static final class ConnectionFactory {
        private final PostgresConnector connector;
        private final AtomicBoolean ready;
        private final AtomicReference<BaseConnection> base;

        public ConnectionFactory(PostgresConnector connector) throws SQLException {
            this.connector = connector;
            this.ready = new AtomicBoolean(true);
            this.base = new AtomicReference<>(connect());
        }

        public BaseConnection open() throws SQLException {
            BaseConnection b = base.get();
            if (b == null) {
                b = connect();
                base.set(b);
                return b;
            } else {
                return b;
            }
        }

        public BaseConnection close() {
            BaseConnection b = base.get();
            if (b != null) {
                try {
                    b.close();
                } catch (Throwable e) {
                }
                base.set(null);
                return b;
            } else {
                return null;
            }
        }

        private BaseConnection connect() throws SQLException {
            return (BaseConnection) connector.getDriver().connect(connector.getUrl(), connector.getProperties());
        }
    }
}
