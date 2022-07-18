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

import java.util.Date;

public interface StatusHistoryReader {

    /**
     * @param connectionId the ID of the Connection for which the Status is
     * desired
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return a {@link StatusHistory} that provides the status information
     * about the Connection with the given ID during the given time period
     */
    StatusHistory getConnectionStatusHistory(String connectionId, Date start, Date end, int preferredDataPoints);

    /**
     * @param processGroupId of group to get status of
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return a {@link StatusHistory} that provides the status information
     * about the Process Group with the given ID during the given time period
     */
    StatusHistory getProcessGroupStatusHistory(String processGroupId, Date start, Date end, int preferredDataPoints);

    /**
     * @param processorId to get status of
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @param includeCounters specifies whether or not metrics from Processor counters
     * should be included in the StatusHistory.
     * @return a {@link StatusHistory} that provides the status information
     * about the Processor with the given ID during the given time period
     */
    StatusHistory getProcessorStatusHistory(String processorId, Date start, Date end, int preferredDataPoints, boolean includeCounters);

    /**
     * @param remoteGroupId to get history of
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return a {@link StatusHistory} that provides the status information
     * about the Remote Process Group with the given ID during the given time
     * period
     */
    StatusHistory getRemoteProcessGroupStatusHistory(String remoteGroupId, Date start, Date end, int preferredDataPoints);

    /**
     * Returns the status history of the actual node.
     *
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     *
     * @return a {@link StatusHistory} that provides the status information
     * about the NiFi node within the specified time range.
     */
    StatusHistory getNodeStatusHistory(Date start, Date end);

    /**
     * Returns the status history of the garbage collection.
     *
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     *
     * @return a {@link GarbageCollectionHistory} that provides the status information
     * about the garbage collection of the given node within the specified time range
     */
    GarbageCollectionHistory getGarbageCollectionHistory(Date start, Date end);
}
