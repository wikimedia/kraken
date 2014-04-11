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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.wikimedia.analytics.kraken.pageview.Pageview;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class ComparePageviewDefinitions extends EvalFunc<Tuple> {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple output;


    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null) {
            return null;
        }

        String url = (String) input.get(0);
        String referer = (String) input.get(1);
        String userAgent = (input.get(2) != null ? (String) input.get(2) : "-");
        String statusCode = (input.get(3) != null ? (String) input.get(3) : "-");
        String ip = (input.get(4) != null ? (String) input.get(4) : "-");
        String mimeType = (input.get(5) != null ? (String) input.get(5) : "-");
        String requestMethod = (input.get(6) != null ? (String) input.get(6) : "-");

        Pageview pageview = new Pageview(url, referer, userAgent, statusCode, ip, mimeType, requestMethod);

        output = tupleFactory.newTuple(3);
        output.set(0, pageview.isPageview() ? 1 : 0);
        output.set(1, pageview.isWebstatscollectorPageview() ? 1 : 0);
        output.set(2, pageview.isWikistatsMobileReportPageview() ? 1 : 0);
        return output;
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        try {
            // Get the types for the column and check them.  If it's
            // wrong figure out what type was passed and give a good error
            // message.
            String msg = "Expected seven chararray input fields, received schema: (";

            List<Boolean> results = new ArrayList<Boolean>();

            for (Schema.FieldSchema inputField : input.getFields()) {
                msg += DataType.findTypeName(inputField.type);
                msg += ",";
                results.add(inputField.type == DataType.CHARARRAY ? true : false);
            }
            msg += ")";
            if (Arrays.asList(results).contains(false)) {
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
        fields.add(new Schema.FieldSchema(null, DataType.INTEGER));   // Pageview definition
        fields.add(new Schema.FieldSchema(null, DataType.INTEGER));   // Webstatscollector definiton
        fields.add(new Schema.FieldSchema(null, DataType.INTEGER));   // Wikistats mobile pageview report definition
        return new Schema(fields);
    }
}
