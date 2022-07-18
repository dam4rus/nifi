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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.nifi.controller.status.history.StandardStatusHistoryDumpService;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryDumpService;
import org.apache.nifi.controller.status.history.StatusStoragesHistoryReader;

public class StatusHistoryFileDumper {

    final StatusStoragesHistoryReader statusStoragesHistoryReader;
    final Collection<StatusHistoryDumpService> statusHistoryDumpServices;
    final String outputDirectory;
    final int days;

    public StatusHistoryFileDumper(final StatusStoragesHistoryReader statusStoragesHistoryReader,
            final Collection<StatusHistoryDumpFactory> dumpFactories,
            final String outputDirectory,
            final int days) {

        this.statusStoragesHistoryReader = statusStoragesHistoryReader;
        this.statusHistoryDumpServices = dumpFactories.stream()
                .map(dumpFactory -> new StandardStatusHistoryDumpService(statusStoragesHistoryReader, dumpFactory))
                .collect(Collectors.toList());
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

    public void dumpComponentsStatusHistories() throws IOException {
        for (final Component component : Component.values()) {
            dumpComponentStatusHistories(component);
        }
    }

    private void dumpComponentStatusHistories(final Component component) throws IOException {
        for (final String id : component.getIds(statusStoragesHistoryReader)) {
            for (final StatusHistoryDumpService statusHistoryDumpService : statusHistoryDumpServices) {
                final String componentOutputDirectory = Paths.get(outputDirectory, id.substring(id.length() - 2)).toString();
                new File(componentOutputDirectory).mkdirs();

                final String fileName = component.filePrefix + "_" + id + "." + statusHistoryDumpService.getFileExtension();
                final String dumpFilePath = Paths.get(componentOutputDirectory, fileName).toString();
                try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                    System.out.println("Dumping component status history to file: " + dumpFilePath);
                    component.dump(statusHistoryDumpService, new ComponentDumpParameters(id, days, outputStream));
                }
            }
        }
    }

    private enum Component {

        PROCESS_GROUP("pg") {
            @Override
            Collection<String> getIds(StatusStoragesHistoryReader statusStoragesHistoryReader) {
                return statusStoragesHistoryReader.getProcessGroupIds();
            }

            @Override
            void dump(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) throws IOException {
                statusHistoryDumpService.dumpProcessGroupStatusHistory(parameters.id, parameters.days, parameters.outputStream);
            }
        },
        PROCESSOR("processor") {
            @Override
            Collection<String> getIds(StatusStoragesHistoryReader statusStoragesHistoryReader) {
                return statusStoragesHistoryReader.getProcessorIds();
            }

            @Override
            void dump(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) throws IOException {
                statusHistoryDumpService.dumpProcessorStatusHistory(parameters.id, parameters.days, parameters.outputStream);
            }
        },
        CONNECTION("connection") {
            @Override
            Collection<String> getIds(StatusStoragesHistoryReader statusStoragesHistoryReader) {
                return statusStoragesHistoryReader.getConnectionIds();
            }

            @Override
            void dump(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) throws IOException {
                statusHistoryDumpService.dumpConnectionStatusHistory(parameters.id, parameters.days, parameters.outputStream);
            }
        },
        REMOTE_PROCESS_GROUP("rpg") {
            @Override
            Collection<String> getIds(StatusStoragesHistoryReader statusStoragesHistoryReader) {
                return statusStoragesHistoryReader.getRemoteProcessGroupIds();
            }

            @Override
            void dump(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) throws IOException {
                statusHistoryDumpService.dumpRemoteProcessGroupStatusHistory(parameters.id, parameters.days, parameters.outputStream);
            }
        };

        abstract Collection<String> getIds(final StatusStoragesHistoryReader statusStoragesHistoryReader);
        abstract void dump(final StatusHistoryDumpService statusHistoryDumpService, final ComponentDumpParameters parameters) throws IOException;

        String filePrefix;

        Component(final String filePrefix) {
            this.filePrefix = filePrefix;
        }
    }
}
