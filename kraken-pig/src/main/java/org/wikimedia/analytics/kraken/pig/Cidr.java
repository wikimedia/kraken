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
import org.wikimedia.analytics.kraken.pageview.CidrFilter;

import java.io.IOException;

/**
 * A pig UDF to filter ipaddresses based on CIDR ranges.
 */
public class Cidr extends EvalFunc<Tuple> {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private Tuple output;
    private CidrFilter cidrFilter;

    /**
     *
     */
    public Cidr(){
        cidrFilter = new CidrFilter();
    }

    /**
     *
     * @param cidrInput
     */
    public Cidr(final String subnetInput){
        cidrFilter = new CidrFilter(subnetInput);
    }

    /**
     *
     * @param input a tuple of size 1 containing the ipadddress from a logline.
     * @return Tuple of size 1 indicating true / false whether ipAddress falls in given CIDR range(s).
     * @throws IOException
     */
    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.get(0) == null) {
            return null;
        }

        String ipAddress = (String) input.get(0);
        boolean result = cidrFilter.ipAddressFallsInRange(ipAddress);

        output = tupleFactory.newTuple(1);
        output.set(0, result);
        return output;
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(input.getField(0));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.TUPLE));
        } catch (Exception e){
            return null;
        }
    }
}
