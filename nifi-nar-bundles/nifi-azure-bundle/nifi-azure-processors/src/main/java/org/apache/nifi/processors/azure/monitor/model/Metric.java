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

import com.azure.monitor.query.models.MetricResult;
import java.util.List;
import java.util.stream.Collectors;

public class Metric {

    private final String id;
    private final String name;
    private final String description;
    private final String resourceType;
    private final String unit;
    private final List<MetricTimeSeries> timeSeries;

    public Metric(final String id,
            final String name,
            final String description,
            final String resourceType,
            final String unit,
            final List<MetricTimeSeries> timeSeries) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.resourceType = resourceType;
        this.unit = unit;
        this.timeSeries = timeSeries;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getResourceType() {
        return resourceType;
    }

    public String getUnit() {
        return unit;
    }

    public List<MetricTimeSeries> getTimeSeries() {
        return timeSeries;
    }

    public static Metric of(MetricResult metricResult) {
        return new Metric(metricResult.getId(),
                metricResult.getMetricName(),
                metricResult.getDescription(),
                metricResult.getResourceType(),
                metricResult.getUnit().toString(),
                metricResult.getTimeSeries().stream().map(MetricTimeSeries::of).collect(Collectors.toList()));
    }
}
