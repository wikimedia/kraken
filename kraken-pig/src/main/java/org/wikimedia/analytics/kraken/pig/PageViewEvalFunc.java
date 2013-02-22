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
package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.pageview.Pageview;
import org.wikimedia.analytics.kraken.pageview.PageviewCanonical;
import org.wikimedia.analytics.kraken.pageview.PageviewFilter;
import org.wikimedia.analytics.kraken.pageview.PageviewType;

/**
 * Entry point for the Pig UDF class that uses the Pageview filter logic.
 * This is a simple Pig script that illustrates how to use this Pig UDF.
 * <code>
 REGISTER 'kraken-pig-0.0.1-SNAPSHOT.jar'
 REGISTER 'kraken-generic-0.0.1-SNAPSHOT.jar'
 SET default_parallism 10;

 DEFINE PAGEVIEW org.wikimedia.analytics.kraken.pig.PageViewEvalFunc();
 DEFINE TO_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');

 LOG_FIELDS     = LOAD '$input' USING PigStorage('\t') AS (
 kafka_offset,
 hostname:chararray,
 udplog_sequence,
 timestamp:chararray,
 request_time:chararray,
 remote_addr:chararray,
 http_status:chararray,
 bytes_sent:chararray,
 request_method:chararray,
 uri:chararray,
 proxy_host:chararray,
 content_type:chararray,
 referer:chararray,
 x_forwarded_for:chararray,
 user_agent:chararray,
 http_language:chararray,
 x_cs:chararray );

 LOG_FIELDS = FILTER LOG_FIELDS BY PAGEVIEW(uri,referer,user_agent,http_status,remote_addr,content_type);

 PARSED     = FOREACH LOG_FIELDS GENERATE TO_DAY(timestamp) AS day, uri;

 GROUPED    = GROUP PARSED BY (day,  uri);

 COUNT       = FOREACH GROUPED GENERATE
 FLATTEN(group) AS (day, uri),
 COUNT_STAR($1) as num PARALLEL 10;
 --DUMP COUNT;
 STORE COUNT into '$output';
 * </code>
 */
public class PageViewEvalFunc extends EvalFunc<Tuple> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     *
     * @param input tuple containing url, referer, userAgent, statusCode, ip and mimeType.
     * @return true/false
     * @throws ExecException
     */
    public final Tuple exec(final Tuple input) throws ExecException {
        if (input == null || input.get(0) == null || input.size() != 6) {
            return null;
        }

        String url = (String) input.get(0);
        String referer = (String) input.get(1);
        String userAgent = (input.get(2) != null ? (String) input.get(2) : "-");
        String statusCode = (input.get(3) != null ? (String) input.get(3) : "-");
        String ip = (input.get(4) != null ? (String) input.get(4) : "-");
        String mimeType = (input.get(5) != null ? (String) input.get(5) : "-");


        Tuple output;
        Pageview pageview = new Pageview(url, referer, userAgent, statusCode, ip, mimeType);

        if (pageview.validate()) {
            output = tupleFactory.newTuple();
            output.set(0, pageview.pageviewCanonical.getLanguage());
            output.set(1, pageview.pageviewCanonical.getProject());
            output.set(2, pageview.pageviewCanonical.getArticleTitle());
        } else {
            output = null;
        }
    return output;
    }
}
