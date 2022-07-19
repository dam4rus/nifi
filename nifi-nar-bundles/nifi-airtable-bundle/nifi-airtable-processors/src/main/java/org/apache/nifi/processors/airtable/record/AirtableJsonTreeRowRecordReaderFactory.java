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

package org.apache.nifi.processors.airtable.record;

import java.io.IOException;
import java.io.InputStream;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordSchema;

public class AirtableJsonTreeRowRecordReaderFactory {

    private static final String STARTING_FIELD_NAME = "records";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSX";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ";

    final ComponentLog componentLog;
    final RecordSchema recordSchema;

    public AirtableJsonTreeRowRecordReaderFactory(final ComponentLog componentLog, final RecordSchema recordSchema) {
        this.componentLog = componentLog;
        this.recordSchema = recordSchema;
    }

    JsonTreeRowRecordReader create(final InputStream recordsStream) throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(
                recordsStream,
                componentLog,
                recordSchema,
                DATE_FORMAT,
                TIME_FORMAT,
                DATE_TIME_FORMAT,
                StartingFieldStrategy.NESTED_FIELD,
                STARTING_FIELD_NAME,
                SchemaApplicationStrategy.SELECTED_PART);
    }
}