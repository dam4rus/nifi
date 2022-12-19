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

import com.azure.monitor.query.models.MetricsQueryResult;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class Metrics {

    private final Integer cost;
    private final String startTime;
    private final String endTime;
    private final String granularity;
    private final String namespace;
    private final String resourceRegion;
    private final List<Metric> metrics;

    public Metrics(final Integer cost,
            final String startTime,
            final String endTime,
            final String granularity,
            final String namespace,
            final String resourceRegion,
            final List<Metric> metrics) {
        this.cost = cost;
        this.startTime = startTime;
        this.endTime = endTime;
        this.granularity = granularity;
        this.namespace = namespace;
        this.resourceRegion = resourceRegion;
        this.metrics = metrics;
    }

    public Integer getCost() {
        return cost;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getGranularity() {
        return granularity;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getResourceRegion() {
        return resourceRegion;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public static Metrics of(final MetricsQueryResult metricsQueryResult) {
        final Duration granularity = metricsQueryResult.getGranularity();
        return new Metrics(metricsQueryResult.getCost(),
                metricsQueryResult.getTimeInterval().getStartTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                metricsQueryResult.getTimeInterval().getEndTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                String.format("%d:%02d:%02d", granularity.toHours() % 24, granularity.toMinutes() % 60, granularity.getSeconds() % 60),
                metricsQueryResult.getNamespace(),
                metricsQueryResult.getResourceRegion(),
                metricsQueryResult.getMetrics().stream().map(Metric::of).collect(Collectors.toList()));
    }
}
