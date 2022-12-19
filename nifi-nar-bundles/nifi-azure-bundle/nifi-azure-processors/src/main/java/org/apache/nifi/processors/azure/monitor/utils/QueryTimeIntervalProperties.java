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

import java.util.concurrent.TimeUnit;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public final class QueryTimeIntervalProperties {

    private QueryTimeIntervalProperties() {

    }

    public static final PropertyDescriptor QUERY_TIME_INTERVAL_STRATEGY = new PropertyDescriptor.Builder()
            .name("query-time-interval-strategy")
            .displayName("Query Time Interval Strategy")
            .description("Strategy to use for the initial query's time interval")
            .allowableValues(QueryTimeIntervalStrategy.class)
            .build();

    public static final PropertyDescriptor QUERY_TIME_INTERVAL = new PropertyDescriptor.Builder()
            .name("query-time-interval")
            .displayName("Query Time Interval")
            .description("The time period for which the logs should be looked up. Must be at least 1 minute")
            .addValidator(StandardValidators.createTimePeriodValidator(1L, TimeUnit.MINUTES, Long.MAX_VALUE, TimeUnit.MINUTES))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .dependsOn(QUERY_TIME_INTERVAL_STRATEGY, QueryTimeIntervalStrategy.DURATION)
            .build();

    public static final PropertyDescriptor QUERY_TIME_WINDOW_LAG = new PropertyDescriptor.Builder()
            .name("query-time-window-lag")
            .displayName("Query Time Window Lag")
            .description("The amount of lag to be applied to the query time window's end point. Set this property to avoid missing records when the clock of your local machines"
                    + " and servers' clock are not in sync. Must be greater than or equal to 1 second.")
            .defaultValue("3 s")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .dependsOn(QUERY_TIME_INTERVAL_STRATEGY)
            .build();
}
