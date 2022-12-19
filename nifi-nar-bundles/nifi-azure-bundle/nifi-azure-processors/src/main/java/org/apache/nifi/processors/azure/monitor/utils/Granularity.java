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

import java.time.Duration;
import org.apache.nifi.components.DescribedValue;

public enum Granularity implements DescribedValue {
    ONE_MINUTE("one-minute", "One Minute", "One minute") {
        @Override
        public Duration getDuration() {
            return Duration.ofMinutes(1);
        }
    },
    FIVE_MINUTES("five-minutes", "Five Minutes", "Five minutes") {
        @Override
        public Duration getDuration() {
            return Duration.ofMinutes(5);
        }
    },
    FIFTEEN_MINUTES("fifteen-minutes", "Fifteen Minutes", "Fifteen minutes") {
        @Override
        public Duration getDuration() {
            return Duration.ofMinutes(15);
        }
    },
    THIRTY_MINUTES("thirty-minutes", "Thirty Minutes", "Thirty minutes") {
        @Override
        public Duration getDuration() {
            return Duration.ofMinutes(30);
        }
    },
    ONE_HOUR("one-hour", "One Hour", "One hour") {
        @Override
        public Duration getDuration() {
            return Duration.ofHours(1);
        }
    },
    SIX_HOURS("six-hours", "Six Hours", "Six hours") {
        @Override
        public Duration getDuration() {
            return Duration.ofHours(6);
        }
    },
    TWELVE_HOURS("twelve-hours", "Twelve Hours", "Twelve hours") {
        @Override
        public Duration getDuration() {
            return Duration.ofHours(12);
        }
    },
    ONE_DAY("one-day", "One Day", "One day") {
        @Override
        public Duration getDuration() {
            return Duration.ofDays(1);
        }
    };

    final String value;
    final String displayName;
    final String description;

    Granularity(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    public abstract Duration getDuration();

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
}
