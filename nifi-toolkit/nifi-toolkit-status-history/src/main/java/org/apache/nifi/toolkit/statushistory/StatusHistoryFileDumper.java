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
import org.apache.nifi.controller.status.history.StatusHistoryDumpService;

public class StatusHistoryFileDumper {

    final Collection<StatusHistoryDumpService> statusHistoryDumpServices;
    final String outputDirectory;
    final int days;

    public StatusHistoryFileDumper(final Collection<StatusHistoryDumpService> statusHistoryDumpService, final String outputDirectory, final int days) {
        this.statusHistoryDumpServices = statusHistoryDumpService;
        this.outputDirectory = outputDirectory;
        this.days = days;
    }

    public void dumpNodeStatusHistory() throws IOException {
        for (final StatusHistoryDumpService statusHistoryDumpService : statusHistoryDumpServices) {
            final String dumpFilePath = Paths.get(outputDirectory, "node_status." + statusHistoryDumpService.getFileExtension()).toString();
            try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                System.out.println("Dumping node status history to file: " + dumpFilePath);
                statusHistoryDumpService.dumpNodeStatusHistory(days, outputStream);
            }
        }
    }

    public void dumpComponentStatusHistories(final Supplier<Collection<String>> idsSupplier,
            final String componentType,
            final BiConsumer<StatusHistoryDumpService, ComponentDumpParameters> dumpFunction)
                throws IOException {
        for (final String id : idsSupplier.get()) {
            for (final StatusHistoryDumpService statusHistoryDumpService : statusHistoryDumpServices) {
                final String dumpFilePath = Paths.get(outputDirectory, componentType + "_" + id + "." + statusHistoryDumpService.getFileExtension()).toString();
                try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                    System.out.println("Dumping component status history to file: " + dumpFilePath);
                    dumpFunction.accept(statusHistoryDumpService, new ComponentDumpParameters(id, days, outputStream));
                }
            }
        }
    }

    public static void dumpProcessGroupStatusHistory(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumpService.dumpProcessGroupStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump process group status history", e);
        }
    }

    public static void dumpProcessorStatusHistory(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumpService.dumpProcessorStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump processor status history", e);
        }
    }

    public static void dumpConnectionStatusHistory(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumpService.dumpConnectionStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump connection status history", e);
        }
    }

    public static void dumpRemoteProcessGroupStatusHistory(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) {
        try {
            statusHistoryDumpService.dumpRemoteProcessGroupStatusHistory(parameters.id, parameters.days, parameters.outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump remote process group status history", e);
        }
    }
}
