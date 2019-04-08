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

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.postgresql.core.BaseConnection;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class PostgresConnectionFactory {
    private final PostgresConnector connector;
    private final AtomicReference<BaseConnection> base;
    private final AtomicBoolean ready;

    public PostgresConnectionFactory(PostgresConnector connector) throws SQLException {
        this.connector = connector;
        this.base = new AtomicReference<>(connect());
        this.ready = new AtomicBoolean(true);
    }

    public PostgresConnector getConnector() {
        return connector;
    }

    public BaseConnection getBase() {
        return base.get();
    }

    public boolean isReady() {
        return ready.get();
    }

    public PostgresConnection createConnection() {
        return null;
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
        BaseConnection b = (BaseConnection) connector.getDriver().connect(connector.getUrl(), connector.getProperties());
        b.setAutoCommit(false);
        return b;
    }
}
