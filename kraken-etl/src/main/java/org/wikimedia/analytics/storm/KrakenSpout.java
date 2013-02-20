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
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class KrakenSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(KrakenSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    private boolean completed = false;
    private FileReader fileReader;

    /**
     *
     */
    public KrakenSpout() {
        this(true);
    }

    /**
     *
     * @param isDistributed
     */
    public KrakenSpout(final boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    /**
     *
     * @param conf
     * @param context
     * @param collector
     */
    public final void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("inputFile").toString());
        } catch (IOException e) {
            throw new RuntimeException("Error reading file " + conf.get("fileInput"));
        }
        this._collector = collector;
    }

    /**
     *
     */
    public void close() {

    }

    /**
     *
     */
    public final void nextTuple() {
        Utils.sleep(100);
        String logLine;
        BufferedReader reader = new BufferedReader(fileReader);

        try {
            while ((logLine = reader.readLine()) != null) {
                _collector.emit(new Values(logLine.split("\t")));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;

        }
//        final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
//        final Random rand = new Random();
//        final String word = words[rand.nextInt(words.length)];
//        _collector.emit(new Values(word));
    }

    /**
     *
     * @param msgId
     */
    public void ack(final Object msgId) {

    }

    /**
     *
     * @param msgId
     */
    public void fail(final Object msgId) {
        System.out.println("ERROR: " + msgId.toString());

    }

    /**
     *
     * @param declarer
     */
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hostname", "udplog_sequence", "timestamp", "responsetime",
                                    "ipaddress", "response", "size", "httpmethod", "url", "x-forwarded-for",
                                    "mimetype", "referrer", "useragent", "", "language", "x-cs"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if (!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }
}
