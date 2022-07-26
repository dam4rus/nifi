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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryAirtableTableIT {

    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        Processor querySObject = new QueryAirtableTable();

        runner = TestRunners.newTestRunner(querySObject);
        runner.setProperty(QueryAirtableTable.API_TOKEN, "???");
    }

    @AfterEach
    void tearDown() {
        runner.shutdown();
    }

    @Test
    void retrievesAndWritesRecords() throws Exception {
        final RecordReaderFactory schemaReader = new MockRecordParser();

        runner.addControllerService("reader", schemaReader);
        runner.enableControllerService(schemaReader);

        final RecordSetWriterFactory writer = new MockRecordWriter();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryAirtableTable.BASE_ID, "appabcdefghijklmn");
        runner.setProperty(QueryAirtableTable.TABLE_ID, "tblabcdefghijklmn");
        runner.setProperty(QueryAirtableTable.SCHEMA_READER, schemaReader.getIdentifier());
        runner.setProperty(QueryAirtableTable.RECORD_WRITER, writer.getIdentifier());

        runner.run();

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(QueryAirtableTable.REL_SUCCESS);

        assertNotNull(results.get(0).getContent());
    }
}
