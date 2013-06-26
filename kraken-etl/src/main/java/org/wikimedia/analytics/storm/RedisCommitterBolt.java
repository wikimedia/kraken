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

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import org.wikimedia.analytics.kraken.pageview.Pageview;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class RedisCommitterBolt implements IRichBolt {
    /** */
    public static final String LAST_COMMITED_TRANSACTION_FIELD = "LAST_COMMIT";

    /** */
    private TransactionAttempt id;

    /** */
    private BatchOutputCollector collector;

    /** */
    private Jedis jedis;

    /** */
    private SimpleDateFormat sdf;

    /**
     *
     * @param timestamp
     * @return
     */
    private String parseTimestamp(final String timestamp) {
        try {
            return this.sdf.parse(timestamp).toString();
        } catch (ParseException e) {
            return "1970-01-01 00:59:59";
        }
    }

    @Override
    public void prepare(final Map map, final TopologyContext topologyContext,
                        final OutputCollector outputCollector) {
        System.out.println("OUTPUT: " + Arrays.toString(map.keySet().toArray()));
        jedis = new Jedis((String) map.get("redisHost"), Integer.parseInt((String) map.get("redisPort")));
        jedis.connect();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    }

    @Override
    public void execute(final Tuple tuple) {

        String ipAddress = tuple.getString(4);
        String statusCode = tuple.getString(5);
        String url = tuple.getString(8);
        String requestMethod = tuple.getString(9);
        String mimeType = tuple.getString(10);
        String referer = tuple.getString(11);
        String userAgent = tuple.getString(13);


        Pageview pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        if (pageview.isPageview()) {
            String timestamp = parseTimestamp(tuple.getString(2));
            //TODO: canonicalize no longer exists pageview.canonicalizeURL();
            //TODO: this needs to be refined.
            //TODO: right now all titles across projects are piled together, not good.
            String key = pageview.getPageviewCanonical().getArticleTitle();
            jedis.hincrBy(key, timestamp, 1);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
