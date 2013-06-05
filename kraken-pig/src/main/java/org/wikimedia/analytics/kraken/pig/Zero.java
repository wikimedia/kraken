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


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.MccMnc;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * This PIG UDF is used to map the custom X-CS http header to the mobile carrier and country
 * and this is part of the Wikipedia Zero project. This function returns two fields:
 * the carrier name and the ISO-3316 country code.
 */

public class ZeroEvalFunc extends EvalFunc<Tuple> {

    /** Map containing x-cs keys and mobile carrier information*/
    private HashMap<String, Schema> mccMncMap = new HashMap<String, Schema>();

    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     *
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public ZeroEvalFunc() throws JsonMappingException, JsonParseException {
        JsonToClassConverter converter = new JsonToClassConverter();
        mccMncMap = converter.construct("org.wikimedia.analytics.kraken.schemas.MccMnc", "mcc_mnc.json", "getMCC_MNC");
    }

    /**
     *
     * @param rawString
     * @return
     */
    private String extractXcsValue(final String rawString) {
        String[] kvpairs = rawString.split(";");
        for (String kvpair : kvpairs) {
            if (kvpair.startsWith("zero")) {
                return kvpair.split("=")[1];
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null) {
            return null;
        }

        String mcc = extractXcsValue((String) input.get(0));
        MccMnc carrier = (MccMnc) this.mccMncMap.get(mcc);
        Tuple output = tupleFactory.newTuple(2);

        if (carrier != null) {
            output.set(0, carrier.getName());
            output.set(1, carrier.getISO());
        } else {
            warn("Key was not found in MccMnc Map: " + mcc, PigWarning.UDF_WARNING_1);
            output.set(0, mcc);
            output.set(1, null);
        }
        return output;
    }

    /**
     *
     * @param input
     * @return
     */
    public final org.apache.pig.impl.logicalLayer.schema.Schema outputSchema(final org.apache.pig.impl.logicalLayer.schema.Schema input) {
        // Check that we were passed two fields
        if (input.size() != 1) {
            throw new RuntimeException("Expected (chararray), input does not have 1 field");
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


        List<org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema> fields = new ArrayList<org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema>();
        // Carrier and ISO country are chararrays
        fields.add(new org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema(null, DataType.CHARARRAY));
        return new org.apache.pig.impl.logicalLayer.schema.Schema(fields);
    }
}
