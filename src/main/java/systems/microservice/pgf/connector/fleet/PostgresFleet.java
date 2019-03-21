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

import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class PostgresFleet implements Iterable<PostgresCluster> {
    private final URL url;
    private final String id;
    private final String application;
    private final ConcurrentMap<String, PostgresCluster> clusterMap;
    private final CopyOnWriteArrayList<PostgresCluster> clusters;

    public PostgresFleet(URL url, String id, String application) {
        this.url = url;
        this.id = id;
        this.application = application;
        this.clusterMap = new ConcurrentHashMap<>(16);
        this.clusters = new CopyOnWriteArrayList<>();
    }

    public URL getUrl() {
        return url;
    }

    public String getId() {
        return id;
    }

    public String getApplication() {
        return application;
    }

    public PostgresCluster getCluster(String id) {
        return clusterMap.get(id);
    }

    @Override
    public Iterator<PostgresCluster> iterator() {
        return clusters.iterator();
    }
}
