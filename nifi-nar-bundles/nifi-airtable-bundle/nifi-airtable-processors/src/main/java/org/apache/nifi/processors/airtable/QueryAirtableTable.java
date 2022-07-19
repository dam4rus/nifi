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

package org.apache.nifi.processors.airtable;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.airtable.model.v0.AirtableRecord;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.processors.airtable.service.ApiVersion;

@PrimaryNodeOnly
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerWhenEmpty
@Tags({"airtable"})
@CapabilityDescription("Query records from an Airtable table")
@Stateful(scopes = Scope.CLUSTER, description = "Records are queried by last modified time to enable incremental loading. The last query's time is stored")
public class QueryAirtableTable extends AbstractProcessor {

    static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
            .name("api-version")
            .displayName("API version")
            .description("API version to use")
            .required(true)
            .defaultValue(ApiVersion.V0.value())
            .allowableValues(Arrays.stream(ApiVersion.values()).map(ApiVersion::value).collect(Collectors.toSet()))
            .build();

    static final PropertyDescriptor BASE_ID = new PropertyDescriptor.Builder()
            .name("base-id")
            .displayName("Base ID")
            .description("ID of the base")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_ID = new PropertyDescriptor.Builder()
            .name("table-id")
            .displayName("Table name/ID")
            .description("Name or ID of the table")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
            .name("api-token")
            .displayName("API token")
            .description("API token to pass to all request")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .displayName("Fields")
            .description("Fields to fetch separated by comma. Leave empty to fetch all field")
            .build();

    static final PropertyDescriptor CUSTOM_FILTER = new PropertyDescriptor.Builder()
            .name("custom-filter")
            .displayName("Custom filter")
            .description("Filter records by a custom formula")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            API_VERSION,
            BASE_ID,
            TABLE_ID,
            API_TOKEN,
            FIELDS,
            CUSTOM_FILTER
    ));

    static final Set<Relationship> RELATIONSHIPS;

    private static final String LAST_QUERY_TIME = "last_query_time";

    private volatile AirtableRestService airtableRestService;

    static {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

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
        final ApiVersion apiVersion = ApiVersion.valueOf(context.getProperty(API_VERSION).getValue().toUpperCase());
        final String apiToken = context.getProperty(API_TOKEN).getValue();
        final String baseId = context.getProperty(BASE_ID).getValue();
        final String tableId = context.getProperty(TABLE_ID).getValue();
        airtableRestService = new AirtableRestService(apiVersion, apiToken, baseId, tableId);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final StateMap state;
        try {
            state = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }
        final String lastQueryTime = state.get(LAST_QUERY_TIME);

        final Collection<AirtableRecord> airtableRecords = airtableRestService.getRecords(lastQueryTime);

        final String nowDateTimeString = OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        final Map<String, String> newState = new HashMap<>(state.toMap());
        newState.put(LAST_QUERY_TIME, nowDateTimeString);
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to update cluster state", e);
        }
    }
}
