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
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 *
 */
public class LocalTopology {
    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;

//    public static class ExclamationBolt extends BaseRichBolt {
//        OutputCollector _collector;
//
//
//        @Override
//        public void prepare(final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector) {
//            _collector = outputCollector;
//
//        }
//
//        @Override
//        public void execute(final Tuple tuple) {
//            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
//            _collector.ack(tuple);
//        }
//
//        @Override
//        public void declareOutputFields(final OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields("word"));
//        }
//    }

    /**
     *
     * @param args CLI arguments
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("inputFile", "/Users/diederik/Development/kraken/kraken-etl/src/main/resources/testdata");
        conf.put("redis-host", REDIS_HOST);
        conf.put("redis-port", REDIS_PORT);


        KrakenSpout spout = new KrakenSpout();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kraken-spout", new KrakenSpout(), 1);
        builder.setBolt("kraken-cleaner", new KrakenInitialFilterAndCleanerBolt(), 1).shuffleGrouping("kraken-spout");
        builder.setBolt("redis", new RedisCommitterBolt()).shuffleGrouping("kraken-cleaner");
//        builder.setBolt("kraken-geocoder", new KrakenGeoBolt(), 1)
//                .shuffleGrouping("kraken-cleaner");
//        builder.setBolt("kraken-anonymizer", new KrakenAnonymousBolt(), 2)
//                .shuffleGrouping("kraken-geocoder");



        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KrakenLocal", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("KrakenLocal");
            cluster.shutdown();
        }
    }
}
