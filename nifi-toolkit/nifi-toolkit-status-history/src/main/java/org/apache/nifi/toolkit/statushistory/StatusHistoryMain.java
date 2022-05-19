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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.controller.status.history.CsvStatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.EmbeddedQuestDbStatusHistoryReader;
import org.apache.nifi.controller.status.history.JsonStatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StandardStatusHistoryDumper;

public class StatusHistoryMain {

    private static final Option OPTION_HELP = Option.builder("h")
            .longOpt("help")
            .desc("Display help/usage info")
            .build();
    private static final Option OPTION_FILE = Option.builder("f")
            .longOpt("file")
            .desc("Output file path")
            .hasArg()
            .argName("file-path")
            .required()
            .build();
    private static final Option OPTION_DAYS = Option.builder("d")
            .longOpt("days")
            .desc("Days to collect history for")
            .hasArg()
            .optionalArg(true)
            .build();
    private static final Option OPTION_PERSIST_LOCATION = Option.builder("l")
            .longOpt("location")
            .desc("Persist location")
            .hasArg()
            .required()
            .build();
    private static final Option OPTION_JSON = Option.builder("json")
            .longOpt("json")
            .desc("Export in JSON format (default)")
            .optionalArg(true)
            .build();
    private static final Option OPTION_CSV = Option.builder("csv")
            .longOpt("csv")
            .desc("Export in CSV format")
            .optionalArg(true)
            .build();
    private static final Option OPTION_DUMP_COMPONENTS = Option.builder("dump_components")
            .longOpt("dump-components")
            .desc("Dump all components' status history as well")
            .optionalArg(true)
            .build();

    private static final Options OPTIONS = new Options();

    static {
        OPTIONS.addOption(OPTION_HELP);
        OPTIONS.addOption(OPTION_FILE);
        OPTIONS.addOption(OPTION_DAYS);
        OPTIONS.addOption(OPTION_PERSIST_LOCATION);
        OPTIONS.addOption(OPTION_JSON);
        OPTIONS.addOption(OPTION_CSV);
        OPTIONS.addOption(OPTION_DUMP_COMPONENTS);
    }

    public static void main(String[] args) throws ParseException, IOException {
        final CommandLine commandLine = new DefaultParser().parse(OPTIONS, args);
        if (commandLine.hasOption(OPTION_HELP.getOpt())) {
            // TODO: Implement
        } else {
            final String filePath = commandLine.getOptionValue(OPTION_FILE.getOpt());
            final String persistLocation = commandLine.getOptionValue(OPTION_PERSIST_LOCATION.getOpt());
            final int days = Optional.ofNullable(commandLine.getOptionValue(OPTION_DAYS.getOpt()))
                    .map(Integer::parseInt)
                    .orElse(1);
            final boolean exportToJson = commandLine.hasOption(OPTION_JSON.getOpt()) || !commandLine.hasOption(OPTION_CSV.getOpt());
            final boolean exportToCsv = commandLine.hasOption(OPTION_CSV.getOpt());
            final boolean dumpComponents = commandLine.hasOption(OPTION_DUMP_COMPONENTS.getOpt());

            final EmbeddedQuestDbStatusHistoryReader embeddedQuestDbStatusReader = new EmbeddedQuestDbStatusHistoryReader(Paths.get(persistLocation));
            final List<StandardStatusHistoryDumper> statusHistoryDumpers = new ArrayList<>();
            if (exportToJson) {
                statusHistoryDumpers.add(new StandardStatusHistoryDumper(embeddedQuestDbStatusReader, new JsonStatusHistoryDumpFactory()));
            }
            if (exportToCsv) {
                statusHistoryDumpers.add(new StandardStatusHistoryDumper(embeddedQuestDbStatusReader, new CsvStatusHistoryDumpFactory()));
            }

            for (final StandardStatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
                final String dumpFilePath = filePath + "_node_status." + statusHistoryDumper.getFileExtension();
                try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                    System.out.println("Dumping node status history to file: " + dumpFilePath);
                    statusHistoryDumper.dumpNodeStatusHistory(days, outputStream);
                }
            }
            if (dumpComponents) {
                for (final String processGroupId : embeddedQuestDbStatusReader.getProcessGroupIds()) {
                    for (final StandardStatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
                        final String dumpFilePath = filePath + "_pg_" + processGroupId + "." + statusHistoryDumper.getFileExtension();
                        try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                            System.out.println("Dumping process group with id " + processGroupId + " to file: " + dumpFilePath);
                            statusHistoryDumper.dumpProcessGroupStatusHistory(processGroupId, days, outputStream);
                        }
                    }
                }
                for (final String processorId : embeddedQuestDbStatusReader.getProcessorIds()) {
                    for (final StandardStatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
                        final String dumpFilePath = filePath + "_processor_" + processorId + "." + statusHistoryDumper.getFileExtension();
                        try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                            System.out.println("Dumping processor with id " + processorId + " to file: " + dumpFilePath);
                            statusHistoryDumper.dumpProcessorStatusHistory(processorId, days, outputStream);
                        }
                    }
                }
                for (final String connectionId : embeddedQuestDbStatusReader.getConnectionIds()) {
                    for (final StandardStatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
                        final String dumpFilePath = filePath + "_connection_" + connectionId + "." + statusHistoryDumper.getFileExtension();
                        try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                            System.out.println("Dumping connection with id " + connectionId + " to file: " + dumpFilePath);
                            statusHistoryDumper.dumpConnectionStatusHistory(connectionId, days, outputStream);
                        }
                    }
                }
                for (final String remoteProcessGroupId : embeddedQuestDbStatusReader.getRemoteProcessGroupIds()) {
                    for (final StandardStatusHistoryDumper statusHistoryDumper : statusHistoryDumpers) {
                        final String dumpFilePath = filePath + "_rpg_" + remoteProcessGroupId + "." + statusHistoryDumper.getFileExtension();
                        try (final FileOutputStream outputStream = new FileOutputStream(dumpFilePath)) {
                            System.out.println("Dumping remote process group with id " + remoteProcessGroupId + " to file: " + dumpFilePath);
                            statusHistoryDumper.dumpRemoteProcessGroupStatusHistory(remoteProcessGroupId, days, outputStream);
                        }
                    }
                }
            }

            System.out.println("Successfully dumped status history to " + filePath);
        }
    }

}
