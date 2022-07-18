/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.controller.status.history.storage;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;

public class StatusStorages {

    private final ProcessorStatusStorage processorStatusStorage;
    private final ComponentStatusStorage<ConnectionStatus> connectionStatusStorage;
    private final ComponentStatusStorage<ProcessGroupStatus> processGroupStatusStorage;
    private final ComponentStatusStorage<RemoteProcessGroupStatus> remoteProcessGroupStatusStorage;
    private final NodeStatusStorage nodeStatusStorage;
    private final GarbageCollectionStatusStorage garbageCollectionStatusStorage;

    public StatusStorages(final ProcessorStatusStorage processorStatusStorage,
            final ComponentStatusStorage<ConnectionStatus> connectionStatusStorage,
            final ComponentStatusStorage<ProcessGroupStatus> processGroupStatusStorage,
            final ComponentStatusStorage<RemoteProcessGroupStatus> remoteProcessGroupStatusStorage,
            final NodeStatusStorage nodeStatusStorage,
            final GarbageCollectionStatusStorage garbageCollectionStatusStorage) {

        this.processorStatusStorage = processorStatusStorage;
        this.connectionStatusStorage = connectionStatusStorage;
        this.processGroupStatusStorage = processGroupStatusStorage;
        this.remoteProcessGroupStatusStorage = remoteProcessGroupStatusStorage;
        this.nodeStatusStorage = nodeStatusStorage;
        this.garbageCollectionStatusStorage = garbageCollectionStatusStorage;
    }

    public ProcessorStatusStorage getProcessorStatusStorage() {
        return processorStatusStorage;
    }

    public ComponentStatusStorage<ConnectionStatus> getConnectionStatusStorage() {
        return connectionStatusStorage;
    }

    public ComponentStatusStorage<ProcessGroupStatus> getProcessGroupStatusStorage() {
        return processGroupStatusStorage;
    }

    public ComponentStatusStorage<RemoteProcessGroupStatus> getRemoteProcessGroupStatusStorage() {
        return remoteProcessGroupStatusStorage;
    }

    public NodeStatusStorage getNodeStatusStorage() {
        return nodeStatusStorage;
    }

    public GarbageCollectionStatusStorage getGarbageCollectionStatusStorage() {
        return garbageCollectionStatusStorage;
    }
}
