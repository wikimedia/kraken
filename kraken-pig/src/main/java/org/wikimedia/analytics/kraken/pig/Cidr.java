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
