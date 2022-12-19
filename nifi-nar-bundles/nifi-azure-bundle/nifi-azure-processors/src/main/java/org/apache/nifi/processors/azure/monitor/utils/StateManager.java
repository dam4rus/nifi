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

package org.apache.nifi.processors.azure.monitor.utils;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class StateManager {

    private static final String LAST_QUERY_TIME_WINDOW_END = "last_query_time_window_end";

    private final StateMap state;

    public StateManager(final StateMap state) {
        this.state = state;
    }

    public static StateManager create(final ProcessSession session) {
        try {
            return new StateManager(session.getState(Scope.CLUSTER));
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }
    }

    public Optional<OffsetDateTime> getLastQueryTimeWindowEnd() {
        return Optional.ofNullable(state.get(LAST_QUERY_TIME_WINDOW_END)).map(OffsetDateTime::parse);
    }

    public void putLastQueryTimeWindowEndIfNotExists(final ProcessSession session, final OffsetDateTime queryEndDateTime) {
        if (!containsLastQueryTimeWindowEnd()) {
            putLastQueryTimeWindowEnd(session, queryEndDateTime);
        }
    }

    public void putLastQueryTimeWindowEnd(final ProcessSession session, final OffsetDateTime queryEndDateTime) {
        final Map<String, String> newState = new HashMap<>(state.toMap());
        newState.put(LAST_QUERY_TIME_WINDOW_END, queryEndDateTime.format(DateTimeFormatter.ISO_DATE_TIME));
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to update cluster state", e);
        }
    }

    public boolean containsLastQueryTimeWindowEnd() {
        return state.get(LAST_QUERY_TIME_WINDOW_END) != null;
    }
}
