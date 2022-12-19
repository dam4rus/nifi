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

package org.apache.nifi.processors.azure.monitor;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.Context;
import com.azure.monitor.query.MetricsQueryClient;
import com.azure.monitor.query.MetricsQueryClientBuilder;
import com.azure.monitor.query.models.AggregationType;
import com.azure.monitor.query.models.MetricsQueryOptions;
import com.azure.monitor.query.models.MetricsQueryResult;
import com.azure.monitor.query.models.QueryTimeInterval;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.monitor.model.Metrics;
import org.apache.nifi.processors.azure.monitor.utils.Aggregation;
import org.apache.nifi.processors.azure.monitor.utils.Granularity;
import org.apache.nifi.processors.azure.monitor.utils.QueryTimeIntervalParameters;
import org.apache.nifi.processors.azure.monitor.utils.QueryTimeIntervalProperties;
import org.apache.nifi.processors.azure.monitor.utils.QueryTimeIntervalStrategy;
import org.apache.nifi.processors.azure.monitor.utils.StateManager;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.services.azure.AzureCredentialsService;

@Tags({"azure", "microsoft", "cloud", "monitor", "metric"})
@CapabilityDescription("Query metric entries from Azure Monitor.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@TriggerWhenEmpty
@PrimaryNodeOnly
@Stateful(scopes = Scope.CLUSTER, description = "The last successful query's time is stored in order to enable incremental loading."
        + " The initial query returns all the metrics in the time interval defined by the selected strategy and each subsequent query's time interval will start from the last query's end of the time window."
        + " State is stored across the cluster, so this Processor can run only on the Primary Node and if a new Primary Node is selected,"
        + " the new node can pick up where the previous one left off without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@DefaultSettings(yieldDuration = "30 sec")
public class QueryAzureMonitorMetrics extends AbstractProcessor {

    public static final PropertyDescriptor CREDENTIALS = new PropertyDescriptor.Builder()
            .name("credentials")
            .displayName("Credentials")
            .description("Controller Service used to obtain Azure credentials")
            .identifiesControllerService(AzureCredentialsService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RESOURCE_URI = new PropertyDescriptor.Builder()
            .name("resource-uri")
            .displayName("Resource URI")
            .description("URI of the resource to query")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor METRIC_NAMES = new PropertyDescriptor.Builder()
            .name("metric-names")
            .displayName("Metric Names")
            .description("Name of the metrics to query")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor GRANULARITY = new PropertyDescriptor.Builder()
            .name("granularity")
            .displayName("Granularity")
            .description("The granularity of the results")
            .allowableValues(Granularity.class)
            .build();

    public static final PropertyDescriptor AGGREGATIONS = new PropertyDescriptor.Builder()
            .name("aggregations")
            .displayName("Aggregations")
            .description("Choose whether to report only the primary aggregation type of a metric or select aggregation types manually")
            .allowableValues(Aggregation.class)
            .defaultValue(Aggregation.METRIC_DEFAULT.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor AGGREGATE_TOTAL = new PropertyDescriptor.Builder()
            .name("aggregate-total")
            .displayName("Aggregate Total")
            .description("Report total aggregation. Please note that not all metric can report this aggregation")
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(AGGREGATIONS, Aggregation.USER_DEFINED)
            .required(true)
            .build();

    public static final PropertyDescriptor AGGREGATE_COUNT = new PropertyDescriptor.Builder()
            .name("aggregate-count")
            .displayName("Aggregate Count")
            .description("Report count aggregation. Please note that not all metric can report this aggregation")
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(AGGREGATIONS, Aggregation.USER_DEFINED)
            .required(true)
            .build();

    public static final PropertyDescriptor AGGREGATE_AVERAGE = new PropertyDescriptor.Builder()
            .name("aggregate-average")
            .displayName("Aggregate Average")
            .description("Report average aggregation. Please note that not all metric can report this aggregation")
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(AGGREGATIONS, Aggregation.USER_DEFINED)
            .required(true)
            .build();

    public static final PropertyDescriptor AGGREGATE_MINIMUM = new PropertyDescriptor.Builder()
            .name("aggregate-minimum")
            .displayName("Aggregate Minimum")
            .description("Report minimum aggregation. Please note that not all metric can report this aggregation")
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(AGGREGATIONS, Aggregation.USER_DEFINED)
            .required(true)
            .build();

    public static final PropertyDescriptor AGGREGATE_MAXIMUM = new PropertyDescriptor.Builder()
            .name("aggregate-maximum")
            .displayName("Aggregate Maximum")
            .description("Report maximum aggregation. Please note that not all metric can report this aggregation")
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(AGGREGATIONS, Aggregation.USER_DEFINED)
            .required(true)
            .build();

    public static final PropertyDescriptor TOP_METRICS = new PropertyDescriptor.Builder()
            .name("top-metrics")
            .displayName("Top Metrics")
            .description("Number of top metrics values to query")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ORDER_BY = new PropertyDescriptor.Builder()
            .name("order-by")
            .displayName("Order By")
            .description("Order metrics by")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor FILTER = new PropertyDescriptor.Builder()
            .name("filter")
            .displayName("Filter")
            .description("Filter metrics by")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor METRIC_NAMESPACE = new PropertyDescriptor.Builder()
            .name("metric-namespace")
            .displayName("Metric Namespace")
            .description("Namespace of the metrics to query")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            CREDENTIALS,
            RESOURCE_URI,
            METRIC_NAMES,
            QueryTimeIntervalProperties.QUERY_TIME_INTERVAL_STRATEGY,
            QueryTimeIntervalProperties.QUERY_TIME_INTERVAL,
            QueryTimeIntervalProperties.QUERY_TIME_WINDOW_LAG,
            GRANULARITY,
            AGGREGATIONS,
            AGGREGATE_TOTAL,
            AGGREGATE_COUNT,
            AGGREGATE_AVERAGE,
            AGGREGATE_MINIMUM,
            AGGREGATE_MAXIMUM,
            TOP_METRICS,
            ORDER_BY,
            FILTER,
            METRIC_NAMESPACE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private volatile MetricsQueryClient metricsQueryClient;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final TokenCredential credentials = context.getProperty(CREDENTIALS)
                .asControllerService(AzureCredentialsService.class)
                .getCredentials();
        metricsQueryClient = new MetricsQueryClientBuilder()
                .credential(credentials)
                .buildClient();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final StateManager stateManager = StateManager.create(session);
        final Pair<Optional<OffsetDateTime>, OffsetDateTime> queryTimeInterval = getQueryTimeInterval(context, stateManager);
        final OffsetDateTime queryEndDateTime = queryTimeInterval.getRight();
        if (queryTimeInterval.getLeft().isPresent()) {
            final OffsetDateTime queryStartDateTime = queryTimeInterval.getLeft().get();
            if (Duration.between(queryStartDateTime, queryEndDateTime).isZero()) {
                stateManager.putLastQueryTimeWindowEndIfNotExists(session, queryEndDateTime);
                context.yield();
                return;
            }
        }

        final MetricsQueryOptions metricsQueryOptions = buildMetricsQueryOptions(context,
                queryTimeInterval.getLeft().map(dateTime -> Pair.of(dateTime, queryTimeInterval.getRight())));
        final String resourceUri = context.getProperty(RESOURCE_URI)
                .evaluateAttributeExpressions()
                .getValue();
        final String metricNames = context.getProperty(METRIC_NAMES)
                .evaluateAttributeExpressions()
                .getValue();
        final MetricsQueryResult queryResult = metricsQueryClient.queryResourceWithResponse(resourceUri,
                        Arrays.asList(metricNames.split(",")),
                        metricsQueryOptions,
                        Context.NONE)
                .getValue();

        stateManager.putLastQueryTimeWindowEnd(session, queryEndDateTime);

        if (queryResult.getMetrics().isEmpty()) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        try {
            OBJECT_MAPPER.writeValue(session.write(flowFile), Metrics.of(queryResult));
        } catch (IOException ex) {
            throw new ProcessException("Failed to write metrics query result to FlowFile", ex);
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }

    private Optional<QueryTimeIntervalParameters> getQueryTimeIntervalStrategyParameters(final ProcessContext context) {
        final QueryTimeIntervalStrategy queryTimeIntervalStrategy = context.getProperty(QueryTimeIntervalProperties.QUERY_TIME_INTERVAL_STRATEGY)
                        .asDescribedValue(QueryTimeIntervalStrategy.class);
        if (queryTimeIntervalStrategy == null) {
            return Optional.empty();
        }
        final Long durationSeconds = context.getProperty(QueryTimeIntervalProperties.QUERY_TIME_INTERVAL)
                .evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.MINUTES);
        final Long queryTimeWindowLag = context.getProperty(QueryTimeIntervalProperties.QUERY_TIME_WINDOW_LAG)
                .evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.SECONDS);
        return Optional.of(new QueryTimeIntervalParameters(queryTimeIntervalStrategy, durationSeconds, queryTimeWindowLag));
    }

    private MetricsQueryOptions buildMetricsQueryOptions(final ProcessContext context,
            final Optional<Pair<OffsetDateTime, OffsetDateTime>> queryTimeInterval) {
        final MetricsQueryOptions metricsQueryOptions = new MetricsQueryOptions();
        queryTimeInterval.ifPresent(interval -> metricsQueryOptions.setTimeInterval(
                new QueryTimeInterval(interval.getLeft(), interval.getRight())));
        Optional.ofNullable(context.getProperty(GRANULARITY).asDescribedValue(Granularity.class))
                .map(Granularity::getDuration)
                .ifPresent(metricsQueryOptions::setGranularity);
        getAggregationTypes(context).ifPresent(metricsQueryOptions::setAggregations);
        Optional.ofNullable(context.getProperty(TOP_METRICS).evaluateAttributeExpressions().asInteger())
                .ifPresent(metricsQueryOptions::setTop);
        Optional.ofNullable(context.getProperty(ORDER_BY).evaluateAttributeExpressions().getValue())
                .ifPresent(metricsQueryOptions::setOrderBy);
        Optional.ofNullable(context.getProperty(FILTER).evaluateAttributeExpressions().getValue())
                .ifPresent(metricsQueryOptions::setFilter);
        Optional.ofNullable(context.getProperty(METRIC_NAMESPACE).evaluateAttributeExpressions().getValue())
                .ifPresent(metricsQueryOptions::setMetricNamespace);
        return metricsQueryOptions;
    }

    private Optional<List<AggregationType>> getAggregationTypes(ProcessContext context) {
        final Aggregation aggregation = context.getProperty(AGGREGATIONS)
                .evaluateAttributeExpressions()
                .asDescribedValue(Aggregation.class);

        if (aggregation != Aggregation.USER_DEFINED) {
            return Optional.empty();
        }

        final List<AggregationType> aggregationTypes = new ArrayList<>();
        if (context.getProperty(AGGREGATE_TOTAL).asBoolean()) {
            aggregationTypes.add(AggregationType.TOTAL);
        }
        if (context.getProperty(AGGREGATE_COUNT).asBoolean()) {
            aggregationTypes.add(AggregationType.COUNT);
        }
        if (context.getProperty(AGGREGATE_AVERAGE).asBoolean()) {
            aggregationTypes.add(AggregationType.AVERAGE);
        }
        if (context.getProperty(AGGREGATE_MINIMUM).asBoolean()) {
            aggregationTypes.add(AggregationType.MINIMUM);
        }
        if (context.getProperty(AGGREGATE_MAXIMUM).asBoolean()) {
            aggregationTypes.add(AggregationType.MAXIMUM);
        }

        return Optional.of(aggregationTypes);
    }

    private Pair<Optional<OffsetDateTime>, OffsetDateTime> getQueryTimeInterval(final ProcessContext context, final StateManager stateManager) {
        final Optional<QueryTimeIntervalParameters> queryTimeIntervalParameters = getQueryTimeIntervalStrategyParameters(context);
        final OffsetDateTime queryEndDateTime = queryTimeIntervalParameters
                .map(QueryTimeIntervalParameters::getWindowLag)
                .map(windowLag -> OffsetDateTime.now().minusSeconds(windowLag))
                .orElseGet(OffsetDateTime::now)
                .truncatedTo(ChronoUnit.MINUTES)
                .withOffsetSameInstant(ZoneOffset.UTC);

        final Optional<OffsetDateTime> lastQueryTimeWindowEnd = stateManager.getLastQueryTimeWindowEnd();
        final Optional<OffsetDateTime> queryStartDateTime = lastQueryTimeWindowEnd.isPresent()
                ? lastQueryTimeWindowEnd
                : queryTimeIntervalParameters.map(parameters -> parameters.getQueryStartDateTime(queryEndDateTime));

        return Pair.of(queryStartDateTime, queryEndDateTime);
    }
}
