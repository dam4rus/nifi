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

import java.time.OffsetDateTime;

public class QueryTimeIntervalParameters {

    private final QueryTimeIntervalStrategy strategy;
    private final Long duration;
    private final Long windowLag;

    public QueryTimeIntervalParameters(final QueryTimeIntervalStrategy strategy, final Long duration, final Long windowLag) {
        this.strategy = strategy;
        this.duration = duration;
        this.windowLag = windowLag;
    }

    public OffsetDateTime getQueryStartDateTime(final OffsetDateTime queryEndDateTime) {
        return strategy.getQueryStartDateTime(this, queryEndDateTime);
    }

    public Long getDuration() {
        return duration;
    }

    public Long getWindowLag() {
        return windowLag;
    }
}
