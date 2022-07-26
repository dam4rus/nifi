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

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.processors.airtable.service.AirtableRestService.API_V0_BASE_URL;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.airtable.record.AirtableRecordSet;
import org.apache.nifi.processors.airtable.record.JsonTreeRowRecordReaderFactory;
import org.apache.nifi.processors.airtable.service.AirtableGetRecordsParameters;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.JsonRecordReaderFactory;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

@PrimaryNodeOnly
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerWhenEmpty
@Tags({"airtable"})
@CapabilityDescription("Query records from an Airtable table")
@Stateful(scopes = Scope.CLUSTER, description = "Records are queried by last modified time to enable incremental loading. The last query's time is stored")
public class QueryAirtableTable extends AbstractProcessor {

    static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("api-url")
            .displayName("API url")
            .description("API url to use")
            .defaultValue(API_V0_BASE_URL)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .required(true)
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
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CUSTOM_FILTER = new PropertyDescriptor.Builder()
            .name("custom-filter")
            .displayName("Custom filter")
            .description("Filter records by a custom formula")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("page-size")
            .displayName("Page size")
            .description("Number of rows to be fetched in a single request")
            .defaultValue("0")
            .addValidator(StandardValidators.createLongValidator(0, 100, true))
            .build();

    static final PropertyDescriptor MAX_RECORDS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("max-records-per-flow-file")
            .displayName("Max Records Per Flow File")
            .description("Max number of records in a flow file")
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor METADATA_STRATEGY = new PropertyDescriptor.Builder()
            .name("metadata-strategy")
            .displayName("Metadata strategy")
            .description("Strategy to use for fetching record schema")
            .required(true)
            .defaultValue(MetadataStrategy.USE_JSON_RECORD_READER.name())
            .allowableValues(buildMetadataStrategyAllowableValues())
            .build();

    static final PropertyDescriptor SCHEMA_READER = new PropertyDescriptor.Builder()
            .name("schema-reader")
            .displayName("Schema reader")
            .description("Record reader to use for schema")
            .dependsOn(METADATA_STRATEGY, MetadataStrategy.USE_JSON_RECORD_READER.name())
            .identifiesControllerService(JsonRecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record writer")
            .description("Record writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor WEB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("web-client-service")
            .displayName("Web client service")
            .description("Web client service provider")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            API_URL,
            BASE_ID,
            TABLE_ID,
            API_TOKEN,
            FIELDS,
            CUSTOM_FILTER,
            PAGE_SIZE,
            MAX_RECORDS_PER_FLOW_FILE,
            METADATA_STRATEGY,
            SCHEMA_READER,
            RECORD_WRITER,
            WEB_CLIENT_SERVICE
    ));

    static final Set<Relationship> RELATIONSHIPS;

    private static final String LAST_RECORD_FETCH_TIME = "last_record_fetch_time";

    private static final int QUERY_LAG_SECONDS = 1;

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
        final String apiUrl = context.getProperty(API_URL).getValue();
        final String apiToken = context.getProperty(API_TOKEN).getValue();
        final String baseId = context.getProperty(BASE_ID).getValue();
        final String tableId = context.getProperty(TABLE_ID).getValue();
        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE).asControllerService(WebClientServiceProvider.class);
        airtableRestService = new AirtableRestService(webClientServiceProvider.getWebClientService(), apiUrl, apiToken, baseId, tableId);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String fieldsProperty = context.getProperty(FIELDS).getValue();
        final String customFilter = context.getProperty(CUSTOM_FILTER).getValue();
        final JsonRecordReaderFactory schemaRecordReaderFactory = context.getProperty(SCHEMA_READER).asControllerService(JsonRecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final Integer pageSize = context.getProperty(PAGE_SIZE).asInteger();
        final Integer maxRecordsPerFlowFile = context.getProperty(MAX_RECORDS_PER_FLOW_FILE).asInteger();

        final StateMap state;
        try {
            state = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }

        final String lastRecordFetchTime = state.get(LAST_RECORD_FETCH_TIME);
        final String nowDateTimeString = OffsetDateTime.now().minusSeconds(QUERY_LAG_SECONDS).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        final AirtableGetRecordsParameters.Builder getRecordsFilterBuilder = new AirtableGetRecordsParameters.Builder();
        if (lastRecordFetchTime != null) {
            getRecordsFilterBuilder
                    .modifiedAfter(lastRecordFetchTime)
                    .modifiedBefore(nowDateTimeString);
        }
        if (fieldsProperty != null) {
            getRecordsFilterBuilder.fields(Arrays.stream(fieldsProperty.split(",")).collect(Collectors.toList()));
        }
        getRecordsFilterBuilder.filterByFormula(customFilter);
        if (pageSize != null && pageSize > 0) {
            getRecordsFilterBuilder.pageSize(pageSize);
        }

        final AirtableGetRecordsParameters getRecordsParameters = getRecordsFilterBuilder.build();
        final String recordsJson = airtableRestService.getRecords(getRecordsParameters);

        final List<FlowFile> flowFiles = new ArrayList<>();
        final FlowFile flowFile = session.create();
        final Map<String, String> originalAttributes = flowFile.getAttributes();

        final RecordSchema recordSchema;
        final RecordSchema writerSchema;
        try {
            final ByteArrayInputStream recordsStream = new ByteArrayInputStream(recordsJson.getBytes(StandardCharsets.UTF_8));
            recordSchema = schemaRecordReaderFactory.createRecordReader(flowFile, recordsStream, getLogger()).getSchema();
            writerSchema = writerFactory.getSchema(originalAttributes, recordSchema);
        } catch (MalformedRecordException | IOException | SchemaNotFoundException e) {
            throw new ProcessException("Couldn't get record schema", e);
        }

        session.remove(flowFile);

        int totalRecordCount = 0;
        final JsonTreeRowRecordReaderFactory recordReaderFactory = new JsonTreeRowRecordReaderFactory(getLogger(), recordSchema);
        try (final AirtableRecordSet airtableRecordSet = new AirtableRecordSet(recordsJson, recordReaderFactory, airtableRestService, getRecordsParameters)) {
            while (true) {
                final AtomicInteger recordCountHolder = new AtomicInteger();
                final Map<String, String> attributes = new HashMap<>();
                FlowFile flowFileToAdd = session.create();
                flowFileToAdd = session.write(flowFileToAdd, out -> {
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writerSchema, out, originalAttributes)) {
                        final RecordSet recordSet = (maxRecordsPerFlowFile > 0 ? airtableRecordSet.limit(maxRecordsPerFlowFile) : airtableRecordSet);
                        final WriteResult writeResult = writer.write(recordSet);

                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());

                        recordCountHolder.set(writeResult.getRecordCount());
                    } catch (SchemaNotFoundException e) {
                        throw new ProcessException("Couldn't read records from input", e);
                    }
                });

                flowFileToAdd = session.putAllAttributes(flowFileToAdd, attributes);

                final int recordCount = recordCountHolder.get();
                totalRecordCount += recordCount;
                if (recordCount > 0) {
                    flowFiles.add(flowFileToAdd);
                } else {
                    session.remove(flowFileToAdd);

                    if (maxRecordsPerFlowFile > 0) {
                        final String fragmentIdentifier = UUID.randomUUID().toString();

                        for (int i = 0; i < flowFiles.size(); i++) {
                            final Map<String, String> fragmentAttributes = new HashMap<>();
                            fragmentAttributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
                            fragmentAttributes.put(FRAGMENT_INDEX.key(), String.valueOf(i));
                            fragmentAttributes.put(FRAGMENT_COUNT.key(), String.valueOf(flowFiles.size()));

                            flowFiles.set(i, session.putAllAttributes(flowFiles.get(i), fragmentAttributes));
                        }
                    }
                    break;
                }
            }
        } catch (IOException e) {
            throw new ProcessException("Couldn't reader records from input", e);
        }

        if (totalRecordCount > 0) {
            session.transfer(flowFiles, REL_SUCCESS);
            session.adjustCounter("Records Processed", totalRecordCount, false);
            final String flowFilesAsString = flowFiles.stream().map(FlowFile::toString).collect(Collectors.joining(", ", "[", "]"));
            getLogger().info("Successfully written {} records for flow files {}", totalRecordCount, flowFilesAsString);

            final Map<String, String> newState = new HashMap<>(state.toMap());
            newState.put(LAST_RECORD_FETCH_TIME, nowDateTimeString);
            try {
                context.getStateManager().setState(newState, Scope.CLUSTER);
            } catch (IOException e) {
                throw new ProcessException("Failed to update cluster state", e);
            }
        } else {
            session.remove(flowFiles);
            context.yield();
        }
    }

    private static AllowableValue[] buildMetadataStrategyAllowableValues() {
        return Arrays.stream(MetadataStrategy.values())
                .map(value -> new AllowableValue(value.name(), value.displayName(), value.description()))
                .toArray(AllowableValue[]::new);
    }
}
