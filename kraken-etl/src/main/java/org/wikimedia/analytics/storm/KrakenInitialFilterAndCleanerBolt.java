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


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 *
 */
public class KrakenInitialFilterAndCleanerBolt implements IRichBolt {

    OutputCollector outputCollector;
    @Override
    public void prepare(final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(final Tuple tuple) {

        String statusCode = parseResponse(tuple.getString(5));
        if (statusCode.startsWith("40") || statusCode.startsWith("50")) {
            return;
        }

        String mimeType = parseMimeType(tuple.getString(10));
        if (mimeType.startsWith("image") || mimeType.endsWith("javascript") || mimeType.endsWith("css")) {
            return;
        }

        String url = tuple.getString(8);
        if (url.contains("bits.wikimedia.org")) {
            return;
        }

        String hostName = parseHostName(tuple.getString(0));
        String sequenceNumber = tuple.getString(1);
        String timeStamp = tuple.getString(2);
        String responseTime = tuple.getString(3);
        String ipAddress = tuple.getString(4);

        String responseSize = tuple.getString(6);
        String requestMethod = tuple.getString(7);

        String peerIp = tuple.getString(9);

        String referer = tuple.getString(11);
        String XForwardedFor = tuple.getString(12);
        String userAgent = parseUserAgent(tuple.getString(13));
        String acceptLanguage = parseHttpLanguage(tuple.getString(14));
        String X_CS = tuple.getString(15);


        outputCollector.emit(new Values(hostName, sequenceNumber, timeStamp,
                responseTime, ipAddress, statusCode, responseSize, requestMethod, url, peerIp,
                mimeType, referer, XForwardedFor, userAgent, acceptLanguage, X_CS));

        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hostname", "udplog_sequence", "timestamp", "responsetime",
                "ipaddress", "response", "size", "httpmethod", "url", "x-forwarded-for",
                "mimetype", "referrer", "useragent", "", "language", "x-cs"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     *
     * @param hostnameInput
     * @return
     */
    private String parseHostName(final String hostnameInput) {
        String[] hostname = hostnameInput.split("\\.");
        if (hostname.length == 1) {
            return hostname[0];
        } else {
            return hostname[0] + hostname[1];
        }
    }

    /**
     *
     * @param mimeTypeInput
     * @return
     */
    private String parseMimeType(final String mimeTypeInput) {
        String[] mimeType = mimeTypeInput.split(" ");
        return mimeType[0].replace(";", "");
    }

    /**
     *
     * @param responseInput
     * @return
     */
    private String parseResponse(final String responseInput) {
        if (responseInput.contains("/")) {
            String[] response = responseInput.split("/");
            return response[1];
        } else {
            return responseInput;
        }
    }

    /**
     *
     * @param userAgentInput
     * @return
     */
    private String parseUserAgent(final String userAgentInput) {
        return userAgentInput.replace("%20", " ");
    }

    /**
     *
     * @param httpLanguageInput
     * @return
     */
    private String parseHttpLanguage(final String httpLanguageInput) {
        String[] languageArray = httpLanguageInput.split(";");
        if (languageArray.length == 1) {
            return languageArray[0];
        } else if (languageArray[0].contains(",")) {
            String[] language = languageArray[0].split(",");
            return language[0];
        }
        return  httpLanguageInput;
    }
}
