/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
 *This program is free software; you can redistribute it and/or
 *modify it under the terms of the GNU General Public License
 *as published by the Free Software Foundation; either version 2
 *of the License, or (at your option) any later version.
 *
 *This program is distributed in the hope that it will be useful,
 *but WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *GNU General Public License for more details.
 *
 *You should have received a copy of the GNU General Public License
 *along with this program; if not, write to the Free Software
 *Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

 */
package org.wikimedia.analytics.storm;

import backtype.storm.Config;
import org.apache.commons.cli.*;

/**
 * A simple interface to start Kraken Storm in local or in Kafka mode.
 */
public class Cli {
    public static final String USAGE =
   "      ___           ___           ___           ___           ___           ___     \n" +
        "     /\\__\\        /\\  \\         /\\  \\         /\\__\\         /\\  \\         /\\__\\    \n" +
        "    /:/  /        /::\\  \\       /::\\  \\       /:/  /        /::\\  \\       /::|  |   \n" +
        "   /:/__/        /:/\\:\\  \\     /:/\\:\\  \\     /:/__/        /:/\\:\\  \\     /:|:|  |   \n" +
        "  /::\\__\\____   /::\\~\\:\\  \\   /::\\~\\:\\  \\   /::\\__\\____   /::\\~\\:\\  \\   /:/|:|  |__ \n" +
        " /:/\\:::::\\__\\ /:/\\:\\ \\:\\__\\ /:/\\:\\ \\:\\__\\ /:/\\:::::\\__\\ /:/\\:\\ \\:\\__\\ /:/ |:| /\\__\\\n" +
        " \\/_|:|~~|~    \\/_|::\\/:/  / \\/__\\:\\/:/  / \\/_|:|~~|~    \\:\\~\\:\\ \\/__/ \\/__|:|/:/  /\n" +
        "     |:|  |        |:|::/  /       \\::/  /     |:|  |      \\:\\ \\:\\__\\       |:/:/  / \n" +
        "     |:|  |        |:|\\/__/        /:/  /      |:|  |       \\:\\ \\/__/       |::/  /  \n" +
        "     |:|  |        |:|  |         /:/  /       |:|  |        \\:\\__\\         /:/  /   \n" +
        "     \\|__|         \\|__|         \\/__/         \\|__|         \\/__/         \\/__/  \n" +
        "\n \n [-mode <local|kafka>] [-input <absolute path to file>] [-redis_host <host>] [-redisport <port>]";
    private static final String HEADER = "Kraken Storm - realtime ETL, Copyright 2012-2013 Wikimedia Foundation licensed under GPL2.\n.";
    private static final String FOOTER = "\nThis program was written by Diederik van Liere <dvanliere@wikimedia.org>\n";

    private String file;
    private Boolean debug = true;
    private String mode = "kafka";

    private String redisHost = "localhost";
    private String redisPort = "6379";

    private String kafkaServers;
    private String kafkaProduct;
    private String kafkaTopic;
    private String kafkaZookeeperPath;
//    private Integer kafkaPartitions;
//    private String kafkaOffsetId;

    /**
     * @param args should contain the options to start the funnel analysis.
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public static void main(final String[] args) throws Exception {
        Cli cli = new Cli();
        CommandLineParser parser = new GnuParser();

        Options options = new Options();

        // General options
        Option help = new Option("help", "print this message");
        Option mode = OptionBuilder
                .withArgName("mode")
                .hasArg()
                .withDescription(
                        "Valid choices are local and kafka.")
                .create("mode");

        Option debug = OptionBuilder.withArgName("debug").withDescription("Turn debug on/off").create("debug");

        // Local mode related options
        Option inputFile = OptionBuilder.withArgName("file").hasArg()
                .withDescription("Specify the full path to the test input dataset (only for local mode).").create("file");


        // Redis related options
        Option redisHost = OptionBuilder.withArgName("redisHost").hasArg()
                .withDescription("Specify the server that is running Redis.").create("redisHost");

        Option redisPort = OptionBuilder.withArgName("redisPort").hasArg()
                .withDescription("Specify the Redis port to connect to.").create("redisPort");


        // Kafka related options
        Option kfServers = OptionBuilder.withArgName("kafkaServers").hasArg()
                .withDescription("Specify the list of kafka servers, separated by comma.")
                .create("kafkaServers");

//        Option kafkaPartitions = OptionBuilder.withArgName("kafkaPartitions").hasArg()
//                .withDescription("Specify the number of Kafka partitions.").create("kafkaPartitions");

        Option kafkaTopic = OptionBuilder.withArgName("kafkaTopic").hasArg()
                .withDescription("Specify the Kafka topic.").create("kafkaTopic");


        Option kafkaZookeeperPath = OptionBuilder.withArgName("kafkaZookeeperPath").hasArg()
                .withDescription("Specify the Kafka topic.").create("kafkaZookeeperPath");

//        Option kafkaOffsetId = OptionBuilder.withArgName("kafkaOffsetId").hasArg()
//                .withDescription("Specify the Kafka topic.").create("kafkaOffsetId");


        Option kafkaProduct = OptionBuilder.withArgName("kafkaProduct").hasArg()
                .withDescription("Specify the Kafka product code.").create("kafkaProduct");



        mode.setRequired(true);
//        redisHost.setRequired(true);
//        redisPort.setRequired(true);

//        OptionGroup groupLocal = new OptionGroup();
//        groupLocal.addOption(inputFile);
//
//        OptionGroup groupKafka = new OptionGroup();
//        groupKafka.addOption(kfServers);
//        groupKafka.addOption(kafkaProduct);
//        groupKafka.addOption(kafkaTopic);
//        groupKafka.addOption(kafkaZookeeperPath);
//        groupKafka.addOption(kafkaPartitions);
//        groupKafka.addOption(kafkaOffsetId);

        options.addOption(mode);
        options.addOption(redisHost);
        options.addOption(redisPort);
        options.addOption(debug);
        options.addOption(help);
        options.addOption(kfServers);
        options.addOption(kafkaProduct);
        options.addOption(kafkaTopic);
        options.addOption(kafkaZookeeperPath);

//        options.addOptionGroup(groupLocal);
//        options.addOptionGroup(groupKafka);


        // automatically generate the help statement
        CommandLine line;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
            if (line.hasOption("mode")) {
                cli.mode = line.getOptionValue("mode", "local");
            }
            if (line.hasOption("file")) {
                cli.file = line.getOptionValue("file");
            }
            if (line.hasOption("kafkaProduct")) {
                cli.kafkaProduct = line.getOptionValue("kafkaProduct");
            }
            if (line.hasOption("kafkaServers")) {
                cli.kafkaServers = line.getOptionValue("kafkaServers");
            }

            if (line.hasOption("kafkaTopic")) {
                cli.kafkaTopic = line.getOptionValue("kafkaTopic");
            }
            if (line.hasOption("kafkaZookeeperPath")) {
                cli.kafkaZookeeperPath= line.getOptionValue("kafkaZookeeperPath");
            }
//            if (line.hasOption("kafkaOffsetId")) {
//                cli.kafkaOffsetId = line.getOptionValue("kafkaOffsetId");
//            }

//            if (line.hasOption("kafkaPartitions")) {
//                cli.kafkaPartitions = Integer.parseInt(line.getOptionValue("kafkaPartitions"));
//            }

            if (line.hasOption("redisHost")) {
                cli.redisHost = line.getOptionValue("redisHost", "localhost");
            }
            if (line.hasOption("redisPort")) {
                cli.redisPort = line.getOptionValue("redisport", "6379");
            }
            if (line.hasOption("debug")) {
                cli.debug = Boolean.parseBoolean(line.getOptionValue("debug", "false"));
            }
            if (line.hasOption("help")) {
                printUsage(options);
                System.exit(-1);
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            printUsage(options);
            System.exit(-1);
        }

        Config conf = new Config();
        conf.setDebug(cli.debug);
        conf.put("mode", cli.mode);
        conf.put("redisHost", cli.redisHost);
        conf.put("redisPort", cli.redisPort);
        if ("local".equals(cli.mode)) {
            if (cli.debug){
                conf.put("inputFile", "/Users/diederik/Development/kraken/kraken-etl/src/main/resources/testdata");
            } else {
                conf.put("inputFile", cli.file);
            }
        } else {
            conf.put("kafkaServers", cli.kafkaServers);
            conf.put("kafkaTopic", cli.kafkaTopic);
            conf.put("kafkaZookeeperPath", cli.kafkaZookeeperPath);
            conf.put("kafkaProduct", cli.kafkaProduct);
//            conf.put("kafkaPartitions", cli.kafkaPartitions);
            //conf.put("kafkaOffsetId", cli.kafkaOffsetId);
        }
        KrakenTopology topology = new KrakenTopology(conf);
        topology.start();
    }

    /**
     *
     * @param options
     */
    private static void printUsage(final Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(90);
        helpFormatter.printHelp(HEADER, USAGE, options, FOOTER);
    }
}
