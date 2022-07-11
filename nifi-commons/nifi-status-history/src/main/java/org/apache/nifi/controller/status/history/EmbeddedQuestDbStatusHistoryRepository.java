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

import java.util.Collection;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.questdb.QuestDbDatabaseManager;
import org.apache.nifi.controller.status.history.storage.BufferedWriterFlushWorker;
import org.apache.nifi.controller.status.history.storage.BufferedWriterForStatusStorage;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EmbeddedQuestDbStatusHistoryRepository implements StatusHistoryRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedQuestDbStatusHistoryRepository.class);
    private static final int PERSIST_BATCH_SIZE = 1000;
    private static final long PERSIST_FREQUENCY = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
    private static final long ROLL_FREQUENCY = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);


    private final InMemoryComponentDetailsStorage componentDetailsProvider = new InMemoryComponentDetailsStorage();
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(3, new BasicThreadFactory.Builder().namingPattern("EmbeddedQuestDbStatusHistoryRepositoryWorker-%d").build());

    private final QuestDbStatusHistoryReader embeddedQuestDbStatusReader;

    private final long persistFrequency;
    private final int daysToKeepNodeData;
    private final int daysToKeepComponentData;

    private final BufferedWriterForStatusStorage<ProcessorStatus> processorStatusWriter;
    private final BufferedWriterForStatusStorage<ConnectionStatus> connectionStatusWriter;
    private final BufferedWriterForStatusStorage<ProcessGroupStatus> processGroupStatusWriter;
    private final BufferedWriterForStatusStorage<RemoteProcessGroupStatus> remoteProcessGroupStatusWriter;
    private final BufferedWriterForStatusStorage<NodeStatus> nodeStatusWriter;
    private final BufferedWriterForStatusStorage<GarbageCollectionStatus> garbageCollectionStatusWriter;

    /**
     * Default no args constructor for service loading only
     */
    public EmbeddedQuestDbStatusHistoryRepository() {
        persistFrequency = PERSIST_FREQUENCY;
        daysToKeepNodeData = -1;
        daysToKeepComponentData = -1;

        embeddedQuestDbStatusReader = null;

        processorStatusWriter = null;
        connectionStatusWriter = null;
        processGroupStatusWriter = null;
        remoteProcessGroupStatusWriter = null;
        nodeStatusWriter = null;
        garbageCollectionStatusWriter = null;
    }

    public EmbeddedQuestDbStatusHistoryRepository(final NiFiProperties niFiProperties) {
        this(niFiProperties, PERSIST_FREQUENCY);
    }

    EmbeddedQuestDbStatusHistoryRepository(final NiFiProperties niFiProperties, final long persistFrequency) {
        embeddedQuestDbStatusReader = new QuestDbStatusHistoryReader(niFiProperties.getQuestDbStatusRepositoryPath(), componentDetailsProvider);

        this.persistFrequency = persistFrequency;
        daysToKeepNodeData = getDaysToKeepNodeData(niFiProperties);
        daysToKeepComponentData = getDaysToKeepComponentData(niFiProperties);

        nodeStatusWriter = new BufferedWriterForStatusStorage<>(embeddedQuestDbStatusReader.getNodeStatusStorage(), PERSIST_BATCH_SIZE);
        garbageCollectionStatusWriter = new BufferedWriterForStatusStorage<>(embeddedQuestDbStatusReader.getGarbageCollectionStatusStorage(), PERSIST_BATCH_SIZE);
        processorStatusWriter = new BufferedWriterForStatusStorage<>(embeddedQuestDbStatusReader.getProcessorStatusStorage(), PERSIST_BATCH_SIZE);
        connectionStatusWriter = new BufferedWriterForStatusStorage<>(embeddedQuestDbStatusReader.getConnectionStatusStorage(), PERSIST_BATCH_SIZE);
        processGroupStatusWriter = new BufferedWriterForStatusStorage<>(embeddedQuestDbStatusReader.getProcessGroupStatusStorage(), PERSIST_BATCH_SIZE);
        remoteProcessGroupStatusWriter = new BufferedWriterForStatusStorage<>(embeddedQuestDbStatusReader.getRemoteProcessGroupStatusStorage(), PERSIST_BATCH_SIZE);
    }

    @Override
    public void start() {
        LOGGER.debug("Starting status history repository");

        final EmbeddedQuestDbRolloverHandler nodeRolloverHandler = new EmbeddedQuestDbRolloverHandler(QuestDbDatabaseManager.getNodeTableNames(),
                daysToKeepNodeData,
                embeddedQuestDbStatusReader.getDbContext());
        final EmbeddedQuestDbRolloverHandler componentRolloverHandler = new EmbeddedQuestDbRolloverHandler(QuestDbDatabaseManager.getComponentTableNames(),
                daysToKeepComponentData,
                embeddedQuestDbStatusReader.getDbContext());
        final BufferedWriterFlushWorker writer = new BufferedWriterFlushWorker(Arrays.asList(
            nodeStatusWriter,
            garbageCollectionStatusWriter,
            processorStatusWriter,
            connectionStatusWriter,
            processGroupStatusWriter,
            remoteProcessGroupStatusWriter
        ));

        scheduledExecutorService.scheduleWithFixedDelay(nodeRolloverHandler, 0, ROLL_FREQUENCY, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleWithFixedDelay(componentRolloverHandler, 0, ROLL_FREQUENCY, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleWithFixedDelay(writer, 0, persistFrequency, TimeUnit.MILLISECONDS);

        LOGGER.debug("Status history repository is started");
    }

    @Override
    public void shutdown() {
        LOGGER.debug("Status history repository started to shut down");
        scheduledExecutorService.shutdown();
        embeddedQuestDbStatusReader.getDbContext().close();
        LOGGER.debug("Status history repository has been shut down");
    }

    @Override
    public void capture(
        final NodeStatus nodeStatus,
        final ProcessGroupStatus rootGroupStatus,
        final List<GarbageCollectionStatus> garbageCollectionStatus,
        final Date capturedAt
    ) {
        captureNodeLevelStatus(nodeStatus, garbageCollectionStatus, capturedAt.toInstant());
        captureComponentLevelStatus(rootGroupStatus, capturedAt.toInstant());
    }

    private void captureComponentLevelStatus(final ProcessGroupStatus rootGroupStatus, final Instant capturedAt) {
        captureComponents(rootGroupStatus, capturedAt);
        updateComponentDetails(rootGroupStatus);
    }

    private void captureNodeLevelStatus(final NodeStatus nodeStatus, final List<GarbageCollectionStatus> garbageCollectionStatus, final Instant capturedAt) {
        nodeStatusWriter.collect(new ImmutablePair<>(capturedAt, nodeStatus));
        garbageCollectionStatus.forEach(s -> garbageCollectionStatusWriter.collect(new ImmutablePair<>(capturedAt, s)));
    }

    private void captureComponents(final ProcessGroupStatus groupStatus, final Instant capturedAt) {
        processGroupStatusWriter.collect(new ImmutablePair<>(capturedAt, groupStatus));
        groupStatus.getConnectionStatus().forEach(s -> connectionStatusWriter.collect(new ImmutablePair<>(capturedAt, s)));
        groupStatus.getRemoteProcessGroupStatus().forEach(s -> remoteProcessGroupStatusWriter.collect(new ImmutablePair<>(capturedAt, s)));
        groupStatus.getProcessorStatus().forEach(s -> processorStatusWriter.collect(new ImmutablePair<>(capturedAt, s)));
        groupStatus.getProcessGroupStatus().forEach(childGroupStatus -> captureComponents(childGroupStatus, capturedAt));
    }

    /**
     * Before the first capture, there will be no component detail provided!
     *
     * @param groupStatus Updates component details for components within the group, including the group itself.
     */
    private void updateComponentDetails(final ProcessGroupStatus groupStatus) {
        // Note: details of deleted components will not be maintained (thus they are not reachable), but their status
        // information is stored in the database until rolled out.
        final Map<String, ComponentDetails> accumulator = new HashMap<>();
        updateComponentDetails(groupStatus, accumulator);
        componentDetailsProvider.setComponentDetails(accumulator);
    }

    private void updateComponentDetails(final ProcessGroupStatus groupStatus, final Map<String, ComponentDetails> accumulator) {
        accumulator.put(groupStatus.getId(), ComponentDetails.forProcessGroup(groupStatus));
        groupStatus.getConnectionStatus().forEach(status -> accumulator.put(status.getId(), ComponentDetails.forConnection(status)));
        groupStatus.getRemoteProcessGroupStatus().forEach(status -> accumulator.put(status.getId(), ComponentDetails.forRemoteProcessGroup(status)));
        groupStatus.getProcessorStatus().forEach(status -> accumulator.put(status.getId(), ComponentDetails.forProcessor(status)));
        groupStatus.getProcessGroupStatus().forEach(childGroupStatus -> updateComponentDetails(childGroupStatus, accumulator));
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        return embeddedQuestDbStatusReader.getConnectionStatusHistory(connectionId, start, end, preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return embeddedQuestDbStatusReader.getProcessGroupStatusHistory(processGroupId, start, end, preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints, final boolean includeCounters) {
        return embeddedQuestDbStatusReader.getProcessorStatusHistory(processorId, start, end, preferredDataPoints, includeCounters);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return embeddedQuestDbStatusReader.getRemoteProcessGroupStatusHistory(remoteGroupId, start, end, preferredDataPoints);
    }

    @Override
    public GarbageCollectionHistory getGarbageCollectionHistory(final Date start, final Date end) {
        return embeddedQuestDbStatusReader.getGarbageCollectionHistory(start, end);
    }

    @Override
    public StatusHistory getNodeStatusHistory(final Date start, final Date end) {
        return embeddedQuestDbStatusReader.getNodeStatusHistory(start, end);
    }

    @Override
    public Collection<String> getProcessGroupIds() {
        return embeddedQuestDbStatusReader.getProcessGroupIds();
    }

    @Override
    public Collection<String> getProcessorIds() {
        return embeddedQuestDbStatusReader.getProcessorIds();
    }

    @Override
    public Collection<String> getConnectionIds() {
        return embeddedQuestDbStatusReader.getConnectionIds();
    }

    @Override
    public Collection<String> getRemoteProcessGroupIds() {
        return embeddedQuestDbStatusReader.getRemoteProcessGroupIds();
    }

    private Integer getDaysToKeepNodeData(final NiFiProperties niFiProperties) {
        return niFiProperties.getIntegerProperty(
                NiFiProperties.STATUS_REPOSITORY_QUESTDB_PERSIST_NODE_DAYS,
                NiFiProperties.DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_NODE_DAYS);
    }

    private Integer getDaysToKeepComponentData(final NiFiProperties niFiProperties) {
        return niFiProperties.getIntegerProperty(
                NiFiProperties.STATUS_REPOSITORY_QUESTDB_PERSIST_COMPONENT_DAYS,
                NiFiProperties.DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_COMPONENT_DAYS);
    }
}
