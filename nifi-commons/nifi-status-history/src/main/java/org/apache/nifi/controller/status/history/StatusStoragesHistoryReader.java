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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import org.apache.nifi.controller.status.history.storage.ProcessorStatusStorage;
import org.apache.nifi.controller.status.history.storage.StatusStorages;

public class StatusStoragesHistoryReader implements StatusHistoryReader {

    private final StatusStorages storages;

    public StatusStoragesHistoryReader(final StatusStorages storages) {
        this.storages = storages;
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        return storages.getConnectionStatusStorage().read(connectionId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return storages.getProcessGroupStatusStorage().read(processGroupId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints, final boolean includeCounters) {
        final ProcessorStatusStorage processorStatusStorage = storages.getProcessorStatusStorage();
        return includeCounters
                ? processorStatusStorage.readWithCounter(processorId, getStartTime(start), getEndTime(end), preferredDataPoints)
                : processorStatusStorage.read(processorId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return storages.getRemoteProcessGroupStatusStorage().read(remoteGroupId, getStartTime(start), getEndTime(end), preferredDataPoints);
    }

    @Override
    public GarbageCollectionHistory getGarbageCollectionHistory(final Date start, final Date end) {
        return storages.getGarbageCollectionStatusStorage().read(getStartTime(start), getEndTime(end));
    }

    @Override
    public StatusHistory getNodeStatusHistory(final Date start, final Date end) {
        return storages.getNodeStatusStorage().read(getStartTime(start), getEndTime(end));
    }

    public Collection<String> getProcessGroupIds() {
        return storages.getProcessGroupStatusStorage().getIds();
    }

    public Collection<String> getProcessorIds() {
        return storages.getProcessorStatusStorage().getIds();
    }

    public Collection<String> getConnectionIds() {
        return storages.getConnectionStatusStorage().getIds();
    }

    public Collection<String> getRemoteProcessGroupIds() {
        return storages.getRemoteProcessGroupStatusStorage().getIds();
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
