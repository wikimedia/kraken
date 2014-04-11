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


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.MccMnc;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.util.HashMap;


/**
 * This PIG UDF is used to map the custom X-CS http header to the mobile carrier and country
 * and this is part of the Wikipedia Zero project. This function returns two fields:
 * the carrier name and the ISO-3316 country code.
 */

public class Zero extends EvalFunc<Tuple> {

    /** Map containing x-cs keys and mobile carrier information*/
    private HashMap<String, Schema> mccMncMap = new HashMap<String, Schema>();

    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     *
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public Zero() throws JsonMappingException, JsonParseException {
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
        if (rawString.matches("\\d\\d\\d-.*")) {
            return rawString;
        } else {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null) {
            return null;
        }

        String mcc = extractXcsValue((String) input.get(0));

        if (mcc == null) {
            return null;
        }

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

        org.apache.pig.impl.logicalLayer.schema.Schema tupleSchema = new org.apache.pig.impl.logicalLayer.schema.Schema();
        tupleSchema.add(new org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema("carrier", DataType.CHARARRAY));
        tupleSchema.add(new org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema("iso", DataType.CHARARRAY));
        org.apache.pig.impl.logicalLayer.schema.Schema ret;
        try {
          ret = new org.apache.pig.impl.logicalLayer.schema.Schema(new org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema(null,tupleSchema, DataType.TUPLE));
        } catch (FrontendException e) {
          throw new RuntimeException(e);
        }
        return ret;
    }
}
