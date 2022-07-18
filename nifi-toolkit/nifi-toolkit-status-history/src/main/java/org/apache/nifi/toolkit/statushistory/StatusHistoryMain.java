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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.controller.status.history.CsvStatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusStoragesHistoryReader;
import org.apache.nifi.controller.status.history.JsonStatusHistoryDumpFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.storage.StatusStorages;
import org.apache.nifi.controller.status.history.storage.questdb.QuestDbStatusStoragesFactory;

public class StatusHistoryMain {

    private static final Option OPTION_HELP = Option.builder("h")
            .longOpt("help")
            .desc("Display help/usage info")
            .build();
    private static final Option OPTION_OUTPUT_DIR = Option.builder("o")
            .longOpt("output-dir")
            .desc("Output directory path")
            .hasArg()
            .argName("output-dir")
            .required()
            .build();
    private static final Option OPTION_DAYS = Option.builder("d")
            .longOpt("days")
            .desc("How many days' status history should be collected. Defaults to 1")
            .hasArg()
            .optionalArg(true)
            .build();
    private static final Option OPTION_PERSIST_LOCATION = Option.builder("l")
            .longOpt("location")
            .desc("Location of the persistent status repository (e.g. /var/lib/nifi/status_repository)")
            .hasArg()
            .required()
            .build();
    private static final Option OPTION_JSON = Option.builder()
            .longOpt("json")
            .desc("Export in JSON format (default)")
            .optionalArg(true)
            .build();
    private static final Option OPTION_CSV = Option.builder()
            .longOpt("csv")
            .desc("Export in CSV format")
            .optionalArg(true)
            .build();
    private static final Option OPTION_DUMP_COMPONENTS = Option.builder()
            .longOpt("dump-components")
            .desc("Dump all components' status history as well")
            .optionalArg(true)
            .build();

    private static final Options OPTIONS = new Options();

    private static final String HELP_HEADER = System.lineSeparator() + "A tool for exporting node and component status history." + System.lineSeparator() + System.lineSeparator();
    private static final String HELP_FOOTER = System.lineSeparator() + "Java home: " +
            System.getenv("JAVA_HOME") + System.lineSeparator() + "NiFi Toolkit home: " + System.getenv("NIFI_TOOLKIT_HOME");

    static {
        OPTIONS.addOption(OPTION_HELP);
        OPTIONS.addOption(OPTION_OUTPUT_DIR);
        OPTIONS.addOption(OPTION_DAYS);
        OPTIONS.addOption(OPTION_PERSIST_LOCATION);
        OPTIONS.addOption(OPTION_JSON);
        OPTIONS.addOption(OPTION_CSV);
        OPTIONS.addOption(OPTION_DUMP_COMPONENTS);
    }

    public static void main(String[] args) throws IOException {
        final CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(OPTIONS, args);
        } catch (ParseException e) {
            System.err.println(e.getLocalizedMessage());
            printHelp();
            return;
        }

        if (commandLine.hasOption(OPTION_HELP.getLongOpt())) {
            printHelp();
            return;
        }

        final String persistLocation = commandLine.getOptionValue(OPTION_PERSIST_LOCATION.getLongOpt());
        if (isPersistLocationInvalid(persistLocation)) {
            return;
        }

        final int days = Optional.ofNullable(commandLine.getOptionValue(OPTION_DAYS.getLongOpt()))
                .map(Integer::parseInt)
                .orElse(1);
        if (days <= 0) {
            System.err.println("Invalid argument: The number of days should be greater than 0");
            return;
        }

        final String outputDirectory = commandLine.getOptionValue(OPTION_OUTPUT_DIR.getLongOpt());
        new File(outputDirectory).mkdirs();

        final boolean exportToJson = commandLine.hasOption(OPTION_JSON.getLongOpt()) || !commandLine.hasOption(OPTION_CSV.getLongOpt());
        final boolean exportToCsv = commandLine.hasOption(OPTION_CSV.getLongOpt());
        final boolean dumpComponents = commandLine.hasOption(OPTION_DUMP_COMPONENTS.getLongOpt());

        final List<StatusHistoryDumpFactory> dumpFactories = new ArrayList<>();
        if (exportToJson) {
            dumpFactories.add(new JsonStatusHistoryDumpFactory());
        }
        if (exportToCsv) {
            dumpFactories.add(new CsvStatusHistoryDumpFactory());
        }

        final QuestDbContext questDbContext = QuestDbContext.ofPersistLocation(Paths.get(persistLocation));
        final StatusStorages statusStorages = QuestDbStatusStoragesFactory.create(questDbContext);
        final StatusStoragesHistoryReader statusStoragesHistoryReader = new StatusStoragesHistoryReader(statusStorages);
        final StatusHistoryFileDumper dumper = new StatusHistoryFileDumper(statusStoragesHistoryReader, dumpFactories, outputDirectory, days);
        dumper.dumpNodeStatusHistory();
        if (dumpComponents) {
            dumper.dumpComponentsStatusHistories();
        }

        System.out.println("Successfully dumped status history to " + outputDirectory);
    }

    private static boolean isPersistLocationInvalid(final String persistLocation) {
        final File persistFile = new File(persistLocation);
        if (!persistFile.exists()) {
            System.err.println("Invalid persist location: Directory does not exists");
            return true;
        }
        if (!persistFile.isDirectory()) {
            System.err.println("Invalid persist location: Not a directory");
            return true;
        }
        return false;
    }

    private static void printHelp() {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.setDescPadding(10);
        helpFormatter.printHelp(StatusHistoryMain.class.getCanonicalName(), HELP_HEADER, OPTIONS, HELP_FOOTER);
    }
}
