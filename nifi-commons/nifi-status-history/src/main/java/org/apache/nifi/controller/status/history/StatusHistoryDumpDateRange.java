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

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

public class StatusHistoryDumpDateRange {

    private final Date start;
    private final Date end;

    public StatusHistoryDumpDateRange(final int days) {
        if (days <= 0) {
            throw new IllegalArgumentException(String.format("The number of days shall be greater than 0. The current value is %s.", days));
        }
        final LocalDateTime endOfToday = LocalDateTime.now().with(LocalTime.MAX);
        final LocalDateTime startOfDaysBefore = endOfToday.minusDays(days).with(LocalTime.MIN);

        start = Date.from(startOfDaysBefore.atZone(ZoneId.systemDefault()).toInstant());
        end = Date.from(endOfToday.atZone(ZoneId.systemDefault()).toInstant());
    }

    public Date getStart() {
        return start;
    }

    public Date getEnd() {
        return end;
    }

}
