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

import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;

import java.util.Date;
import java.util.List;

/**
 * A repository for storing and retrieving node and components' historical status
 * information
 */
public interface StatusHistoryRepository extends StatusHistoryReader {

    String COMPONENT_DETAIL_ID = "Id";
    String COMPONENT_DETAIL_GROUP_ID = "Group Id";
    String COMPONENT_DETAIL_NAME = "Name";
    String COMPONENT_DETAIL_TYPE = "Type";
    String COMPONENT_DETAIL_SOURCE_NAME = "Source Name";
    String COMPONENT_DETAIL_DESTINATION_NAME = "Destination Name";
    String COMPONENT_DETAIL_URI = "Uri";

    /**
     * Captures the status information provided in the given report, providing a
     * timestamp that indicates the time at which the status report was
     * generated. This can be used to replay historical values.
     *
     * @param nodeStatus status of the node
     * @param rootGroupStatus status of the root group and it's content
     * @param timestamp timestamp of capture
     * @param garbageCollectionStatus status of garbage collection
     */
    void capture(NodeStatus nodeStatus, ProcessGroupStatus rootGroupStatus, List<GarbageCollectionStatus> garbageCollectionStatus, Date timestamp);

    /**
     * Starts necessary resources needed for the repository.
     */
    void start();

    /**
     * Stops the resources used by the repository.
     */
    void shutdown();
}
