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

package systems.microservice.pgf.connector.fleet;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class PostgresNode implements Iterable<PostgresNode> {
    private final PostgresCluster cluster;
    private final String id;
    private final UUID uuid;
    private final AtomicBoolean enabled;
    private final AtomicBoolean active;
    private final PostgresNode master;
    private final ConcurrentMap<String, PostgresNode> replicaMap;
    private final CopyOnWriteArrayList<PostgresNode> replicas;

    PostgresNode(PostgresCluster cluster, String id, UUID uuid, AtomicBoolean enabled, AtomicBoolean active, PostgresNode master) {
        this.cluster = cluster;
        this.id = id;
        this.uuid = uuid;
        this.enabled = enabled;
        this.active = active;
        this.master = master;
        this.replicaMap = new ConcurrentHashMap<>(16);
        this.replicas = new CopyOnWriteArrayList<>();
    }

    public PostgresCluster getCluster() {
        return cluster;
    }

    public String getId() {
        return id;
    }

    public UUID getUuid() {
        return uuid;
    }

    public boolean getEnabled() {
        return enabled.get();
    }

    public boolean getActive() {
        return active.get();
    }

    public PostgresNode getMaster() {
        return master;
    }

    public PostgresNode getReplica(String id) {
        return replicaMap.get(id);
    }

    @Override
    public Iterator<PostgresNode> iterator() {
        return replicas.iterator();
    }
}
