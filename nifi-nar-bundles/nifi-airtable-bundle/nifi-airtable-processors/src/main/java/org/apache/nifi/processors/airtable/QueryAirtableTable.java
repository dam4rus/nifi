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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.processors.airtable.service.ApiVersion;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

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
            .defaultValue(ApiVersion.V0.name())
            .allowableValues(Arrays.stream(ApiVersion.values()).map(ApiVersion::name).collect(Collectors.toSet()))
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
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record writer")
            .description("Record writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
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
            CUSTOM_FILTER,
            METADATA_STRATEGY,
            SCHEMA_READER,
            RECORD_WRITER
    ));

    static final Set<Relationship> RELATIONSHIPS;

    private static final String LAST_QUERY_TIME = "last_query_time";
    private static final String STARTING_FIELD_NAME = "records";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSX";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ";

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
        final ApiVersion apiVersion = ApiVersion.valueOf(context.getProperty(API_VERSION).getValue());
        final String apiToken = context.getProperty(API_TOKEN).getValue();
        final String baseId = context.getProperty(BASE_ID).getValue();
        final String tableId = context.getProperty(TABLE_ID).getValue();
        airtableRestService = new AirtableRestService(apiVersion, apiToken, baseId, tableId);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String fieldsProperty = context.getProperty(FIELDS).getValue();
        final String customFilter = context.getProperty(CUSTOM_FILTER).getValue();
        final RecordReaderFactory recordReaderFactory = context.getProperty(SCHEMA_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final StateMap state;
        try {
            state = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }
        final String lastQueryTime = state.get(LAST_QUERY_TIME);

        final List<String> fields = fieldsProperty != null
                ? Arrays.stream(fieldsProperty.split(",")).collect(Collectors.toList())
                : null;
        final String records = airtableRestService.getRecords(lastQueryTime, fields, customFilter);

        FlowFile flowFile = session.create();
        final Map<String, String> originalAttributes = flowFile.getAttributes();

        final RecordSchema recordSchema;
        try {
            final ByteArrayInputStream recordsStream = new ByteArrayInputStream(records.getBytes(StandardCharsets.UTF_8));
            recordSchema = recordReaderFactory.createRecordReader(flowFile, recordsStream, getLogger()).getSchema();
        } catch (MalformedRecordException | IOException | SchemaNotFoundException e) {
            throw new ProcessException("Couldn't get record schema", e);
        }

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCountHolder = new AtomicInteger();
        flowFile = session.write(flowFile, out -> {
            final ByteArrayInputStream recordsStream = new ByteArrayInputStream(records.getBytes(StandardCharsets.UTF_8));
            try (final AirtableRecordIO recordIo = new AirtableRecordIO(recordsStream, recordSchema, writerFactory, out, originalAttributes)) {
                final WriteResult writeResult = recordIo.writeRecordSet();

                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), recordIo.writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());

                recordCountHolder.set(writeResult.getRecordCount());

                final String nowDateTimeString = OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                final Map<String, String> newState = new HashMap<>(state.toMap());
                newState.put(LAST_QUERY_TIME, nowDateTimeString);
                try {
                    context.getStateManager().setState(newState, Scope.CLUSTER);
                } catch (IOException e) {
                    throw new ProcessException("Failed to update cluster state", e);
                }
            } catch (MalformedRecordException | SchemaNotFoundException e) {
                throw new ProcessException("Couldn't read records from input", e);
            }
        });

        int recordCount = recordCountHolder.get();

        if (recordCount == 0) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("Records Processed", recordCount, false);
            getLogger().info("Successfully written {} records for {}", recordCount, flowFile);
        }
    }

    private static AllowableValue[] buildMetadataStrategyAllowableValues() {
        return Arrays.stream(MetadataStrategy.values())
                .map(value -> new AllowableValue(value.name(), value.displayName(), value.description()))
                .toArray(AllowableValue[]::new);
    }

    class AirtableRecordIO implements Closeable {

        final JsonTreeRowRecordReader jsonReader;
        final RecordSetWriter writer;

        AirtableRecordIO(final InputStream recordsStream,
                final RecordSchema recordSchema,
                final RecordSetWriterFactory writerFactory,
                final OutputStream out,
                final Map<String, String> originalAttributes)
                    throws IOException, MalformedRecordException, SchemaNotFoundException {
            jsonReader = new JsonTreeRowRecordReader(
                    recordsStream,
                    getLogger(),
                    recordSchema,
                    DATE_FORMAT,
                    TIME_FORMAT,
                    DATE_TIME_FORMAT,
                    StartingFieldStrategy.NESTED_FIELD,
                    STARTING_FIELD_NAME,
                    SchemaApplicationStrategy.SELECTED_PART);

            writer = writerFactory.createWriter(
                    getLogger(),
                    writerFactory.getSchema(originalAttributes, recordSchema),
                    out,
                    originalAttributes);
        }

        WriteResult writeRecordSet() throws IOException, MalformedRecordException {
            writer.beginRecordSet();
            Record record;
            while ((record = jsonReader.nextRecord()) != null) {
                writer.write(record);
            }
            return writer.finishRecordSet();
        }

        @Override
        public void close() throws IOException {
            jsonReader.close();
            writer.close();
        }
    }
}
