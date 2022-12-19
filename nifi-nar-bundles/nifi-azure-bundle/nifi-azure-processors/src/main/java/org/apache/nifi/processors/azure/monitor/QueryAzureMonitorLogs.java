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
import com.azure.monitor.query.LogsQueryClient;
import com.azure.monitor.query.LogsQueryClientBuilder;
import com.azure.monitor.query.models.LogsQueryOptions;
import com.azure.monitor.query.models.LogsQueryResult;
import com.azure.monitor.query.models.LogsTableCell;
import com.azure.monitor.query.models.QueryTimeInterval;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.monitor.model.Logs;
import org.apache.nifi.processors.azure.monitor.utils.QueryTimeIntervalParameters;
import org.apache.nifi.processors.azure.monitor.utils.QueryTimeIntervalProperties;
import org.apache.nifi.processors.azure.monitor.utils.QueryTimeIntervalStrategy;
import org.apache.nifi.processors.azure.monitor.utils.StateManager;
import org.apache.nifi.services.azure.AzureCredentialsService;

@Tags({"azure", "microsoft", "cloud", "monitor", "log"})
@CapabilityDescription("Query log entries from Azure Monitor.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@TriggerWhenEmpty
@PrimaryNodeOnly
public class QueryAzureMonitorLogs extends AbstractProcessor {

    public static final PropertyDescriptor CREDENTIALS = new PropertyDescriptor.Builder()
            .name("credentials")
            .displayName("Credentials")
            .description("Controller Service used to obtain Azure credentials")
            .identifiesControllerService(AzureCredentialsService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor WORKSPACE_ID = new PropertyDescriptor.Builder()
            .name("workspace-id")
            .displayName("Workspace ID")
            .description("The ID of the workspace from which logs are queried")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("query")
            .displayName("Query")
            .description("Kusto Query to execute")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor QUERY_TIME_INTERVAL_STRATEGY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(QueryTimeIntervalProperties.QUERY_TIME_INTERVAL_STRATEGY)
            .required(true)
            .build();

    public static final PropertyDescriptor ADDITIONAL_WORKSPACES = new PropertyDescriptor.Builder()
            .name("additional-workspaces")
            .displayName("Additional Workspaces")
            .description("Additional workspaces to fetch logs from")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor TIMEOUT_SECONDS = new PropertyDescriptor.Builder()
            .name("timeout")
            .displayName("Timeout Seconds")
            .description("Request timeout in seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            CREDENTIALS,
            WORKSPACE_ID,
            QUERY,
            QUERY_TIME_INTERVAL_STRATEGY,
            QueryTimeIntervalProperties.QUERY_TIME_INTERVAL,
            QueryTimeIntervalProperties.QUERY_TIME_WINDOW_LAG,
            ADDITIONAL_WORKSPACES,
            TIMEOUT_SECONDS
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private volatile LogsQueryClient logsQueryClient;

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
        logsQueryClient = new LogsQueryClientBuilder()
                .credential(credentials)
                .buildClient();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String additionalWorkspaces = context.getProperty(ADDITIONAL_WORKSPACES)
                .evaluateAttributeExpressions()
                .getValue();
        final Long serverTimeout = context.getProperty(TIMEOUT_SECONDS)
                .evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.SECONDS);

        final LogsQueryOptions logsQueryOptions = new LogsQueryOptions();
        if (additionalWorkspaces != null) {
            logsQueryOptions.setAdditionalWorkspaces(additionalWorkspaces.split(","));
        }
        if (serverTimeout != null) {
            logsQueryOptions.setServerTimeout(Duration.ofSeconds(serverTimeout));
        }

        final String workspaceId = context.getProperty(WORKSPACE_ID)
                .evaluateAttributeExpressions()
                .getValue();
        final String query = context.getProperty(QUERY)
                .evaluateAttributeExpressions()
                .getValue();

        final StateManager stateManager = StateManager.create(session);
        final QueryTimeIntervalParameters queryTimeIntervalParameters = getQueryTimeIntervalStrategyParameters(context);
        final OffsetDateTime currentQueryDateTime = OffsetDateTime.now()
                .minusSeconds(queryTimeIntervalParameters.getWindowLag())
                .truncatedTo(ChronoUnit.MINUTES)
                .withOffsetSameInstant(ZoneOffset.UTC);

        final OffsetDateTime queryStartDateTime = stateManager.getLastQueryTimeWindowEnd()
                .orElseGet(() -> queryTimeIntervalParameters.getQueryStartDateTime(currentQueryDateTime));
        final QueryTimeInterval queryTimeInterval = new QueryTimeInterval(queryStartDateTime, currentQueryDateTime);
        final LogsQueryResult queryResults = logsQueryClient.queryWorkspaceWithResponse(workspaceId,
                        query,
                        queryTimeInterval,
                        logsQueryOptions,
                        Context.NONE)
                .getValue();

        stateManager.putLastQueryTimeWindowEnd(session, currentQueryDateTime);

        final List<Map<String, String>> logs = queryResults.getAllTables().stream()
                .flatMap(table -> table.getRows().stream())
                .map(row -> row.getRow().stream().collect(Collectors.toMap(
                        LogsTableCell::getColumnName,
                        cell -> String.valueOf(cell.getValueAsString()))))
                .collect(Collectors.toList());

        FlowFile flowFile = session.create();
        try {
            OBJECT_MAPPER.writeValue(session.write(flowFile), new Logs(logs));
        } catch (IOException ex) {
            throw new ProcessException("Failed to write logs query result to FlowFile", ex);
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }

    private QueryTimeIntervalParameters getQueryTimeIntervalStrategyParameters(final ProcessContext context) {
        final QueryTimeIntervalStrategy queryTimeIntervalStrategy = context.getProperty(QUERY_TIME_INTERVAL_STRATEGY)
                .asDescribedValue(QueryTimeIntervalStrategy.class);
        final Long durationSeconds = context.getProperty(QueryTimeIntervalProperties.QUERY_TIME_INTERVAL)
                .evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.MINUTES);
        final Long queryTimeWindowLag = context.getProperty(QueryTimeIntervalProperties.QUERY_TIME_WINDOW_LAG)
                .evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.SECONDS);
        return new QueryTimeIntervalParameters(queryTimeIntervalStrategy, durationSeconds, queryTimeWindowLag);
    }
}
