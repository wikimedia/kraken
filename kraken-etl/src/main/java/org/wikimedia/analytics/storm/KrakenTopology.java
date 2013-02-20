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
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * This class builds the Kraken Storm topology. The topology is structured as follows:
 *
 * 1) A spout consuming data from either Kafka or file from disk
 * 2) The KrakenInitialFilterAndCleanerBolt, this will filter data that we never need
 * and cleanup some fields to save diskspace and make future processing easier
 * 3) A split: one stream goes to Redis as a replacement for webstatscollector the other stream
 * continues to HDFS (this is not yet fully implemented).
 * 4) The KrakenGeoBolt geocodes every received logline at the country level (not yet implemented)
 * 5) The KrakenAnonymousBolt removes ip information and replaces that with a cryptographic hash. (not yet implemented)
 * 6) The KrakenHDFS writer (not yet implemented)
 */
public class KrakenTopology {
    public Config conf;

    /**
     *
     * @param conf
     */
    public KrakenTopology(final Config conf) {
        this.conf = conf;
    }

    /**
     *
     * @param kafkaServers
     * @return
     */
    private List parseKafkaServers(final String kafkaServers) {
        List<String> kafkaServerList = new ArrayList<String>();
        for (String server : kafkaServers.split(",")) {
            kafkaServerList.add(server);
        }
        return kafkaServerList;
    }
    /**
     *
     * @throws Exception
     */
    public final void start() throws Exception {
        String mode = (String) conf.get("mode");
        TopologyBuilder builder = new TopologyBuilder();

        if ("kafka".equals(mode)) {
            List<String> kafkaServerList = parseKafkaServers((String) conf.get("kafkaServers"));
            SpoutConfig spoutConfig = new SpoutConfig(
                    KafkaConfig.StaticHosts.fromHostString(kafkaServerList, 1), // list of Kafka brokers
                    (String) conf.get("kafkaTopic"), // topic to read from
                    (String) conf.get("kafkaZookeeperPath"), // the root path in Zookeeper for the spout to store the consumer offsets
                    (String) conf.get("kafkaProduct")); // an id for this consumer for storing the consumer offsets in Zookeeper
            KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
            builder.setSpout("kraken-spout", kafkaSpout, 1);
        } else {
            builder.setSpout("kraken-spout", new KrakenSpout(), 1);
        }


        builder.setBolt("kraken-cleaner", new KrakenInitialFilterAndCleanerBolt(), 1).shuffleGrouping("kraken-spout");
        builder.setBolt("redis", new RedisCommitterBolt()).shuffleGrouping("kraken-cleaner");
//        builder.setBolt("kraken-geocoder", new KrakenGeoBolt(), 1)
//                .shuffleGrouping("kraken-cleaner");
//        builder.setBolt("kraken-anonymizer", new KrakenAnonymousBolt(), 2)
//                .shuffleGrouping("kraken-geocoder");




        if ("kafka".equals(mode)) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology("Kraken", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KrakenLocal", this.conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("KrakenLocal");
            cluster.shutdown();
        }
    }
}
