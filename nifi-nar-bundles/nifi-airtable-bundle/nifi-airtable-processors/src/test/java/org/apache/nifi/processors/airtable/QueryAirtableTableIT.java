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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryAirtableTableIT {

    public static final String RECORDS_JSON_BODY = "{\"records\":[{"
            + "\"id\":\"recabcdefghijklmn\","
            + "\"createdTime\":\"1970-00-01T00:00:00.000Z\","
            + "\"fields\":{\"foo\":\"bar\"}}]}";
    public static final String RECORDS_WITH_OFFSET_JSON_BODY = "{\"records\":[{"
            + "\"id\":\"recabcdefghijklmn\","
            + "\"createdTime\":\"1970-00-01T00:00:00.000Z\","
            + "\"fields\":{\"foo\":\"bar\"}}],"
            + "\"offset\":\"ofsabcdefghijklmn\"}";
    public static final String EXPECTED_RECORD_CONTENT =
            "\"recabcdefghijklmn\",\"1970-00-01T00:00:00.000Z\",\"MapRecord[{foo=bar}]\"\n";

    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        final Processor queryAirtableTable = new QueryAirtableTable();

        runner = TestRunners.newTestRunner(queryAirtableTable);

        final RecordReaderFactory schemaReader = new JsonTreeReader();
        runner.addControllerService("reader", schemaReader);
        runner.setProperty(schemaReader, JsonTreeReader.STARTING_FIELD_NAME, "records");
        runner.setProperty(schemaReader, JsonTreeReader.STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.getValue());
        runner.setProperty(schemaReader, JsonTreeReader.SCHEMA_APPLICATION_STRATEGY, SchemaApplicationStrategy.SELECTED_PART.getValue());
        runner.enableControllerService(schemaReader);

        final RecordSetWriterFactory writer = new MockRecordWriter();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        final WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();
        runner.addControllerService("webClientService", webClientServiceProvider);
        runner.enableControllerService(webClientServiceProvider);

        runner.setProperty(QueryAirtableTable.API_TOKEN, "???");
        runner.setProperty(QueryAirtableTable.BASE_ID, "appabcdefghijklmn");
        runner.setProperty(QueryAirtableTable.TABLE_ID, "tblabcdefghijklmn");
        runner.setProperty(QueryAirtableTable.SCHEMA_READER, schemaReader.getIdentifier());
        runner.setProperty(QueryAirtableTable.RECORD_WRITER, writer.getIdentifier());
        runner.setProperty(QueryAirtableTable.WEB_CLIENT_SERVICE_PROVIDER, webClientServiceProvider.getIdentifier());

    }

    @AfterEach
    void tearDown() {
        runner.shutdown();
    }

    @Test
    void retrievesAndWritesRecords() throws Exception {
        try (final MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(RECORDS_JSON_BODY));

            server.start();
            final HttpUrl httpUrl = server.url("/v0/airtable");

            runner.setProperty(QueryAirtableTable.API_URL, httpUrl.toString());
            runner.run();

            final List<MockFlowFile> results = runner.getFlowFilesForRelationship(QueryAirtableTable.REL_SUCCESS);
            assertEquals(1, results.size());
            final MockFlowFile flowFile = results.get(0);
            assertEquals("1", flowFile.getAttribute("record.count"));
            final String content = flowFile.getContent();
            assertEquals(EXPECTED_RECORD_CONTENT, content);
        }
    }

    @Test
    void retrievesAndWritesPagedRecords() throws Exception {
        try (final MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(RECORDS_WITH_OFFSET_JSON_BODY));
            server.enqueue(new MockResponse().setBody(RECORDS_JSON_BODY));

            server.start();
            final HttpUrl httpUrl = server.url("/v0/airtable");

            runner.setProperty(QueryAirtableTable.API_URL, httpUrl.toString());
            runner.run();

            final List<MockFlowFile> results = runner.getFlowFilesForRelationship(QueryAirtableTable.REL_SUCCESS);
            assertEquals(1, results.size());
            final MockFlowFile flowFile = results.get(0);
            assertEquals("2", flowFile.getAttribute("record.count"));
            final String content = flowFile.getContent();
            assertEquals(EXPECTED_RECORD_CONTENT + EXPECTED_RECORD_CONTENT, content);
        }
    }

    @Test
    void retrievesAndWritesPagedRecordsInMultipleFlowFiles() throws Exception {
        try (final MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(RECORDS_WITH_OFFSET_JSON_BODY));
            server.enqueue(new MockResponse().setBody(RECORDS_JSON_BODY));

            server.start();
            final HttpUrl httpUrl = server.url("/v0/airtable");

            runner.setProperty(QueryAirtableTable.MAX_RECORDS_PER_FLOW_FILE, "1");
            runner.setProperty(QueryAirtableTable.API_URL, httpUrl.toString());
            runner.run();

            final List<MockFlowFile> results = runner.getFlowFilesForRelationship(QueryAirtableTable.REL_SUCCESS);
            assertEquals(2, results.size());
            final MockFlowFile firstFlowFile = results.get(0);
            assertEquals("1", firstFlowFile.getAttribute("record.count"));
            final String firstContent = firstFlowFile.getContent();
            assertEquals(EXPECTED_RECORD_CONTENT, firstContent);

            final MockFlowFile secondFlowFile = results.get(1);
            assertEquals("1", secondFlowFile.getAttribute("record.count"));
            final String secondContent = secondFlowFile.getContent();
            assertEquals(EXPECTED_RECORD_CONTENT, secondContent);
        }
    }

    @Test
    void doesNotWriteEmptyRecords() throws Exception {
        try (final MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody("{\"records\":[]}"));

            server.start();
            final HttpUrl httpUrl = server.url("/v0/airtable");

            runner.setProperty(QueryAirtableTable.API_URL, httpUrl.toString());
            runner.run();

            final List<MockFlowFile> results = runner.getFlowFilesForRelationship(QueryAirtableTable.REL_SUCCESS);
            assertTrue(results.isEmpty());
        }
    }
}
