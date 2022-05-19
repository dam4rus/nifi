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

package org.apache.nifi.controller.status.history;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbDatabaseManager;
import org.apache.nifi.controller.status.history.storage.ComponentStatusStorage;
import org.apache.nifi.controller.status.history.storage.GarbageCollectionStatusStorage;
import org.apache.nifi.controller.status.history.storage.NodeStatusStorage;
import org.apache.nifi.controller.status.history.storage.ProcessorStatusStorage;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbConnectionStatusStorage;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbGarbageCollectionStatusStorage;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbNodeStatusStorage;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbProcessGroupStatusStorage;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbProcessorStatusStorage;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbRemoteProcessGroupStatusStorage;

public class EmbeddedQuestDbStatusHistoryReader implements StatusHistoryReader {

    private final QuestDbContext dbContext;
    private final ProcessorStatusStorage processorStatusStorage;
    private final ComponentStatusStorage<ConnectionStatus> connectionStatusStorage;
    private final ComponentStatusStorage<ProcessGroupStatus> processGroupStatusStorage;
    private final ComponentStatusStorage<RemoteProcessGroupStatus> remoteProcessGroupStatusStorage;
    private final NodeStatusStorage nodeStatusStorage;
    private final GarbageCollectionStatusStorage garbageCollectionStatusStorage;

    public EmbeddedQuestDbStatusHistoryReader(final Path persistLocation) {
        this(persistLocation, new InMemoryComponentDetailsStorage());
    }

    public EmbeddedQuestDbStatusHistoryReader(final Path persistLocation, final InMemoryComponentDetailsStorage componentDetailsProvider) {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(persistLocation.toString());
        QuestDbDatabaseManager.checkDatabaseStatus(persistLocation);

        dbContext = new QuestDbContext(new CairoEngine(configuration));

        nodeStatusStorage = new QuestDbNodeStatusStorage(dbContext);
        garbageCollectionStatusStorage = new QuestDbGarbageCollectionStatusStorage(dbContext);
        processorStatusStorage = new QuestDbProcessorStatusStorage(dbContext, componentDetailsProvider);
        connectionStatusStorage = new QuestDbConnectionStatusStorage(dbContext, componentDetailsProvider);
        processGroupStatusStorage = new QuestDbProcessGroupStatusStorage(dbContext, componentDetailsProvider);
        remoteProcessGroupStatusStorage = new QuestDbRemoteProcessGroupStatusStorage(dbContext, componentDetailsProvider);
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        return connectionStatusStorage.read(connectionId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return processGroupStatusStorage.read(processGroupId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints, final boolean includeCounters) {
        return includeCounters
                ? processorStatusStorage.readWithCounter(processorId, getStartTime(start), getEndTime(end), preferredDataPoints)
                : processorStatusStorage.read(processorId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return remoteProcessGroupStatusStorage.read(remoteGroupId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public GarbageCollectionHistory getGarbageCollectionHistory(final Date start, final Date end) {
        return garbageCollectionStatusStorage.read(getStartTime(start), getEndTime(end));
    }

    @Override
    public StatusHistory getNodeStatusHistory(final Date start, final Date end) {
        return nodeStatusStorage.read(getStartTime(start), getEndTime(end));
    }

    @Override
    public Collection<String> getProcessGroupIds() {
        return processGroupStatusStorage.getIds();
    }

    @Override
    public Collection<String> getProcessorIds() {
        return processorStatusStorage.getIds();
    }

    @Override
    public Collection<String> getConnectionIds() {
        return connectionStatusStorage.getIds();
    }

    @Override
    public Collection<String> getRemoteProcessGroupIds() {
        return remoteProcessGroupStatusStorage.getIds();
    }

    public QuestDbContext getDbContext() {
        return dbContext;
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

    private Instant getStartTime(final Date start) {
        if (start == null) {
            return Instant.now().minus(1, ChronoUnit.DAYS);
        } else {
            return start.toInstant();
        }
    }

    private Instant getEndTime(final Date end) {
        return (end == null) ? Instant.now() : end.toInstant();
    }
}
