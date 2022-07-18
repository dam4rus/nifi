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

package org.apache.nifi.controller.status.history;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;

final class CsvStatusHistoryDump implements StatusHistoryDump {

    private final StatusHistory statusHistory;

    CsvStatusHistoryDump(final StatusHistory statusHistory) {
        this.statusHistory = statusHistory;
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {
        final StatusHistoryDTO statusHistoryDto = StatusHistoryUtil.createStatusHistoryDTO(statusHistory);
        if (statusHistoryDto.getAggregateSnapshots().isEmpty()) {
            return;
        }

        final CsvSchema.Builder schemaBuilder = CsvSchema.builder().addColumn("timestamp");
        statusHistoryDto.getAggregateSnapshots()
                .get(0)
                .getStatusMetrics()
                .keySet()
                .forEach(schemaBuilder::addColumn);
        final CsvSchema csvSchema = schemaBuilder.build().withHeader();
        final List<List<Long>> values = statusHistoryDto.getAggregateSnapshots()
                .stream()
                .map(statusSnapshotDTO -> {
                    final List<Long> list = new ArrayList<>();
                    list.add(statusSnapshotDTO.getTimestamp().getTime());
                    list.addAll(statusSnapshotDTO.getStatusMetrics().values());
                    return list;
                })
                .collect(Collectors.toList());
        final DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        new CsvMapper().writer(prettyPrinter)
                .with(csvSchema)
                .writeValues(out)
                .writeAll(values);
    }
}
