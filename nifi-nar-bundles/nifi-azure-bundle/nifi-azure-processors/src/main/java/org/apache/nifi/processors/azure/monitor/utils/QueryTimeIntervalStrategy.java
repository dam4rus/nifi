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
import java.util.Objects;
import org.apache.nifi.components.DescribedValue;

public enum QueryTimeIntervalStrategy implements DescribedValue {
    DURATION("duration", "Duration", "Query entries from the specified duration until now") {
        @Override
        public OffsetDateTime getQueryStartDateTime(final QueryTimeIntervalParameters parameters,
                final OffsetDateTime queryEndDateTime) {
            return queryEndDateTime.minusMinutes(Objects.requireNonNull(parameters.getDuration()));
        }
    },
    NOW("now", "Now", "Only query entries from now") {
        @Override
        public OffsetDateTime getQueryStartDateTime(final QueryTimeIntervalParameters parameters,
                final OffsetDateTime queryEndDateTime) {
            return queryEndDateTime;
        }
    };

    private final String value;
    private final String displayName;
    private final String description;

    QueryTimeIntervalStrategy(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public abstract OffsetDateTime getQueryStartDateTime(final QueryTimeIntervalParameters parameters,
            final OffsetDateTime queryEndDateTime);

}
