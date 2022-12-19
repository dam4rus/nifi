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

package org.apache.nifi.processors.azure.monitor.model;

import com.azure.monitor.query.models.MetricValue;
import java.time.format.DateTimeFormatter;

public class MetricTimeSeriesElement {

    private final String timeStamp;
    private final Double total;
    private final Double minimum;
    private final Double average;
    private final Double count;

    public MetricTimeSeriesElement(final String timeStamp,
            final Double total,
            final Double minimum,
            final Double average,
            final Double count) {
        this.timeStamp = timeStamp;
        this.total = total;
        this.minimum = minimum;
        this.average = average;
        this.count = count;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public Double getTotal() {
        return total;
    }

    public Double getMinimum() {
        return minimum;
    }

    public Double getAverage() {
        return average;
    }

    public Double getCount() {
        return count;
    }

    public static MetricTimeSeriesElement of(MetricValue metricValue) {
        return new MetricTimeSeriesElement(metricValue.getTimeStamp().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                metricValue.getTotal(),
                metricValue.getMinimum(),
                metricValue.getAverage(),
                metricValue.getCount());
    }
}
