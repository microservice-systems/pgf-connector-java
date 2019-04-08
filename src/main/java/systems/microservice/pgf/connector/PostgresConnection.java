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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.util.PGobject;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class PostgresConnection implements Connection, PGConnection {
    private final PostgresConnector connector;
    private final PostgresConnectionFactory factory;
    private final BaseConnection base;

    PostgresConnection(PostgresConnector connector, PostgresConnectionFactory factory, BaseConnection base) {
        this.connector = connector;
        this.factory = factory;
        this.base = base;
    }

    public PostgresConnector getConnector() {
        return connector;
    }

    public PostgresConnectionFactory getFactory() {
        return factory;
    }

    public BaseConnection getBase() {
        return base;
    }

    @Override
    public PostgresStatement createStatement() throws SQLException {
        return new PostgresStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public Array createArrayOf(String typeName, Object elements) throws SQLException {
        return null;
    }

    @Override
    public PGNotification[] getNotifications() throws SQLException {
        return new PGNotification[0];
    }

    @Override
    public PGNotification[] getNotifications(int timeoutMillis) throws SQLException {
        return new PGNotification[0];
    }

    @Override
    public CopyManager getCopyAPI() throws SQLException {
        return null;
    }

    @Override
    public LargeObjectManager getLargeObjectAPI() throws SQLException {
        return null;
    }

    @Override
    public Fastpath getFastpathAPI() throws SQLException {
        return null;
    }

    @Override
    public void addDataType(String type, String className) {

    }

    @Override
    public void addDataType(String type, Class<? extends PGobject> klass) throws SQLException {

    }

    @Override
    public void setPrepareThreshold(int threshold) {

    }

    @Override
    public int getPrepareThreshold() {
        return 0;
    }

    @Override
    public void setDefaultFetchSize(int fetchSize) throws SQLException {

    }

    @Override
    public int getDefaultFetchSize() {
        return 0;
    }

    @Override
    public int getBackendPID() {
        return 0;
    }

    @Override
    public String escapeIdentifier(String identifier) throws SQLException {
        return null;
    }

    @Override
    public String escapeLiteral(String literal) throws SQLException {
        return null;
    }

    @Override
    public PreferQueryMode getPreferQueryMode() {
        return null;
    }

    @Override
    public AutoSave getAutosave() {
        return null;
    }

    @Override
    public void setAutosave(AutoSave autoSave) {

    }

    @Override
    public PGReplicationConnection getReplicationAPI() {
        return null;
    }
}
