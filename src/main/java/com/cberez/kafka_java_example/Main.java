package com.cberez.kafka_java_example;

import com.cberez.kafka_java_example.conf.Mode;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author César Berezowski
 */
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws ParseException, IOException {
        Options opts = new Options();
        opts.addOption("m", "mode", true, "`read` for listening mode (consume & print) / `write` for writing mode (produce content of input-dir)");
        opts.addOption("t", "topic", true, "Topic name to listen / write to");
        opts.addOption("i", "input-dir", true, "Path to files to read and write to topic in writing mode");
        opts.addOption("ks", "kafka-servers", true, "Kafka servers address (comma-separated list of server:port)");
        opts.addOption("h", "help", false, "Print usage");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);

        if (cmd.hasOption("h")) {
            printHelpAndExit(opts, 0);
        }

        Arrays.asList("mode", "kafka-servers", "topic").forEach(param -> {
            if (!cmd.hasOption(param)) {
                logger.error(String.format("Missing parameter %s", param));
                printHelpAndExit(opts, 1);
            }
        });

        Mode mode = Mode.valueOf(cmd.getOptionValue("mode").toUpperCase());
        String topic = cmd.getOptionValue("topic");
        String kafkaServers = cmd.getOptionValue("ks");

        switch(mode) {
            case READ:
                logger.info(String.format("Launching read mode on topic %s with kafka-servers %s", topic, kafkaServers));
                Jobs.read(kafkaServers, topic);
                break;

            case WRITE:
                if (!cmd.hasOption("input-dir")) {
                    logger.error("Missing parameter input-dir for write mode");
                    printHelpAndExit(opts, 1);
                }

                String inputDir = cmd.getOptionValue("input-dir");
                logger.info(String.format("Launching write mode on topic %s with kafka-servers %s with input-dir %s", topic, kafkaServers, inputDir));
                Jobs.write(kafkaServers, topic, inputDir);
                break;
        }
    }

    /**
     * Print job usage and exit with given exit code
     *
     * @param opts     {@link Options} instance
     * @param exitCode int value
     */
    private static void printHelpAndExit(Options opts, int exitCode) {
        HelpFormatter f = new HelpFormatter();
        f.printHelp("Usage", opts);
        System.exit(exitCode);
    }
}
