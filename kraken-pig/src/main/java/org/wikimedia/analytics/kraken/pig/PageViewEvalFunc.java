/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.wikimedia.analytics.kraken.pageview.ProjectInfo;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Entry point for the Pig UDF class that uses the Pageview filter logic.
 * This is a simple Pig script that illustrates how to use this Pig UDF.
 *
 * TODO: This example is for PageViewFilterFunc!
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

 LOG_FIELDS = FILTER LOG_FIELDS BY PAGEVIEW(uri,referer,user_agent,http_status,remote_addr,content_type,request_method);

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
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple output;

    private URL url;

    /**
     *
     * @param input containing url
     * @return (language, project, site_version)
     * @throws ExecException
     */
    public final Tuple exec(final Tuple input) throws ExecException {
        if (input == null || input.get(0) == null) {
            return null;
        }

        output = tupleFactory.newTuple(3);
        try {
            url = new URL((String) input.get(0));

            ProjectInfo projectInfo = new ProjectInfo(url.getHost());
            output.set(0, projectInfo.getLanguage());
            output.set(1, projectInfo.getProjectDomain());
            output.set(2, projectInfo.getSiteVersion());
        } catch (MalformedURLException e) {
            return null;
        }
        return output;
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        // Check that we were passed one fields
        if (input.size() != 1) {
            throw new RuntimeException(
                    "Expected (chararray), input does not have 1 field.");
        }

        try {
            // Get the types for the column and check them.  If it's
            // wrong figure out what type was passed and give a good error
            // message.
            if (input.getField(0).type != DataType.CHARARRAY) {
                String msg = "Expected input (chararray), received schema (";
                msg += DataType.findTypeName(input.getField(0).type);
                msg += ")";
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("language", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("project", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("site_version", DataType.CHARARRAY));
        Schema ret;
        try {
          ret = new Schema(new Schema.FieldSchema("page_view", tupleSchema, DataType.TUPLE));
        } catch (FrontendException e) {
          throw new RuntimeException(e);
        }
        return ret;
    }
}
