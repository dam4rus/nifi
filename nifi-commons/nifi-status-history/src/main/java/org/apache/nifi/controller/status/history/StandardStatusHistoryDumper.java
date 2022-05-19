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

import java.io.IOException;
import java.io.OutputStream;

public class StandardStatusHistoryDumper implements StatusHistoryDumper {

    private final StatusHistoryReader statusHistoryReader;
    private final StatusHistoryDumpFactory statusHistoryDumpFactory;

    public StandardStatusHistoryDumper(final StatusHistoryReader statusHistoryReader, final StatusHistoryDumpFactory statusHistoryDumpFactory) {
        this.statusHistoryReader = statusHistoryReader;
        this.statusHistoryDumpFactory = statusHistoryDumpFactory;
    }

    @Override
    public void dumpNodeStatusHistory(final int days, final OutputStream os) throws IOException {
        final StatusHistoryDumpDateRange dateRange = new StatusHistoryDumpDateRange(days);
        final StatusHistory statusHistory = statusHistoryReader.getNodeStatusHistory(dateRange.getStart(), dateRange.getEnd());
        dumpStatusHistory(statusHistoryDumpFactory, os, statusHistory);
    }

    @Override
    public void dumpProcessGroupStatusHistory(final String processGroupId, final int days, final OutputStream os)
            throws IOException {
        final StatusHistoryDumpDateRange dateRange = new StatusHistoryDumpDateRange(days);
        final StatusHistory statusHistory = statusHistoryReader.getProcessGroupStatusHistory(processGroupId, dateRange.getStart(), dateRange.getEnd(), Integer.MAX_VALUE);
        dumpStatusHistory(statusHistoryDumpFactory, os, statusHistory);
    }

    @Override
    public void dumpProcessorStatusHistory(final String processorId, final int days, final OutputStream os)
            throws IOException {
        final StatusHistoryDumpDateRange dateRange = new StatusHistoryDumpDateRange(days);
        final StatusHistory statusHistory = statusHistoryReader.getProcessorStatusHistory(processorId, dateRange.getStart(), dateRange.getEnd(), Integer.MAX_VALUE, true);
        dumpStatusHistory(statusHistoryDumpFactory, os, statusHistory);
    }

    @Override
    public void dumpConnectionStatusHistory(final String connectionId, final int days, final OutputStream os)
            throws IOException {
        final StatusHistoryDumpDateRange dateRange = new StatusHistoryDumpDateRange(days);
        final StatusHistory statusHistory = statusHistoryReader.getConnectionStatusHistory(connectionId, dateRange.getStart(), dateRange.getEnd(), Integer.MAX_VALUE);
        dumpStatusHistory(statusHistoryDumpFactory, os, statusHistory);
    }

    @Override
    public void dumpRemoteProcessGroupStatusHistory(final String remoteProcessGroupId, final int days, final OutputStream os)
            throws IOException {
        final StatusHistoryDumpDateRange dateRange = new StatusHistoryDumpDateRange(days);
        final StatusHistory statusHistory = statusHistoryReader.getRemoteProcessGroupStatusHistory(remoteProcessGroupId, dateRange.getStart(), dateRange.getEnd(), Integer.MAX_VALUE);
        dumpStatusHistory(statusHistoryDumpFactory, os, statusHistory);
    }

    @Override
    public String getFileExtension() {
        return statusHistoryDumpFactory.getFileExtension();
    }

    private void dumpStatusHistory(final StatusHistoryDumpFactory statusHistoryDumpFactory, final OutputStream os, final StatusHistory statusHistory) throws IOException {
        statusHistoryDumpFactory.create(statusHistory).writeTo(os);
    }
}
