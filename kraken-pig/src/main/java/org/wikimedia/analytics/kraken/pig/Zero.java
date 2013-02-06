/**
 *Copyright (C) 2012  Wikimedia Foundation
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
 *
 * @version $Id: $Id
 */

package org.wikimedia.analytics.kraken.pig;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;


public class Zero extends EvalFunc<Tuple> {

    HashMap<String, Mcc_Mnc> mcc_mnc = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    public HashMap<String, Mcc_Mnc> constructMccMncData() throws JsonMappingException, JsonParseException {
        JsonFactory jfactory = new JsonFactory();
        HashMap<String, Mcc_Mnc> mcc_mnc = new HashMap<String, Mcc_Mnc>();
        List<Mcc_Mnc> zeros = null;

        InputStream input =
                Mcc_Mnc.class.getClassLoader().getResourceAsStream("mcc_mnc.json");
        try {
            JsonParser jParser = jfactory.createJsonParser(input);
            zeros = new ObjectMapper().readValue(jParser,new TypeReference<List<Mcc_Mnc>>() {});
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                input.close();
            } catch (IOException ee){
                System.err.println("Could not close filestream");
            }
        }

        for(Mcc_Mnc zero : zeros) {
            mcc_mnc.put(zero.getMCC_MNC(), zero);
        }
        return mcc_mnc;
    }

    public Zero() throws JsonMappingException, JsonParseException {
        mcc_mnc = this.constructMccMncData();
    }

    /**
     * This PIG UDF expects the X-CS http header as input and will return
     * two fields: the carrier name and the ISO-3316 country code.
     */
    /** {@inheritDoc} */
    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1) {
            return null;
        }
        String key = (String) input.get(0);
        Mcc_Mnc zero = mcc_mnc.get(key);
        if (zero != null) {
            Tuple output = tupleFactory.newTuple(2);
            output.set(0, zero.getName());
            output.set(1, zero.getISO());
            return output;

        } else {
            return null;
        }
    }
}
