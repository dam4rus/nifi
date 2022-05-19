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

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

public class StatusHistoryDumpDateRangeTest {

    private static final String EXPECTED_EXCEPTION_MESSAGE = "The number of days shall be greater than 0. The current value is %s.";

    @Test
    public void testStatusHistoryDumpDateRangeWithZeroOrNegativeDays() {
        final int zeroDays = 0;
        final int negativeDays = -1;

        final IllegalArgumentException zeroDaysException = Assert.assertThrows(IllegalArgumentException.class,
                () -> new StatusHistoryDumpDateRange(zeroDays)
        );

        assertEquals(String.format(EXPECTED_EXCEPTION_MESSAGE, zeroDays), zeroDaysException.getMessage());

        final IllegalArgumentException negativeDaysException = Assert.assertThrows(IllegalArgumentException.class,
                () -> new StatusHistoryDumpDateRange(negativeDays)
        );

        assertEquals(String.format(EXPECTED_EXCEPTION_MESSAGE, negativeDays), negativeDaysException.getMessage());
    }
}
