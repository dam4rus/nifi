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

package org.apache.nifi.toolkit.statushistory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.nifi.controller.status.history.StatusHistoryDumper;

public class StatusHistoryFileDumper {

    final Collection<StatusHistoryDumper> statusHistoryDumpers;
    final String outputDirectory;
    final int days;

    public StatusHistoryFileDumper(final Collection<StatusHistoryDumper> statusHistoryDumpers, final String outputDirectory, final int days) {
        this.statusHistoryDumpers = statusHistoryDumpers;
        this.outputDirectory = outputDirectory;
        this.days = days;
    }

    public void dumpNodeStatusHistory() throws IOException {
        for (final StatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
            final String dumpFilePath = Paths.get(outputDirectory, "node_status." + statusHistoryDumper.getFileExtension()).toString();
            try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                System.out.println("Dumping node status history to file: " + dumpFilePath);
                statusHistoryDumper.dumpNodeStatusHistory(days, outputStream);
            }
        }
    }

    public void dumpComponentStatusHistories(final Supplier<Collection<String>> idsSupplier,
            final String componentType,
            final BiConsumer<StatusHistoryDumper, ComponentDumpParameters> dumpFunction)
                throws IOException {
        for (final String id : idsSupplier.get()) {
            for (final StatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
                final String dumpFilePath = Paths.get(outputDirectory, componentType + "_" + id + "." + statusHistoryDumper.getFileExtension()).toString();
                try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                    System.out.println("Dumping component status history to file: " + dumpFilePath);
                    dumpFunction.accept(statusHistoryDumper, new ComponentDumpParameters(id, days, outputStream));
                }
            }
        }
    }

    public static void dumpProcessGroupStatusHistory(final StatusHistoryDumper statusHistoryDumper, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumper.dumpProcessGroupStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump process group status history", e);
        }
    }

    public static void dumpProcessorStatusHistory(final StatusHistoryDumper statusHistoryDumper, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumper.dumpProcessorStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump processor status history", e);
        }
    }

    public static void dumpConnectionStatusHistory(final StatusHistoryDumper statusHistoryDumper, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumper.dumpConnectionStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump connection status history", e);
        }
    }

    public static void dumpRemoteProcessGroupStatusHistory(final StatusHistoryDumper statusHistoryDumper, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumper.dumpRemoteProcessGroupStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump remote process group status history", e);
        }
    }
}
