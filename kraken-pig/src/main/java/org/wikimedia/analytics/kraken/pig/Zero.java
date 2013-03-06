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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.MccMnc;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This PIG UDF is used to map the custom X-CS http header to the mobile carrier and country
 * and this is part of the Wikipedia Zero project. This function returns two fields:
 * the carrier name and the ISO-3316 country code.
 */

public class Zero extends EvalFunc<Tuple> {

    /** Map containing x-cs keys and mobile carrier information*/
    private HashMap<String, Schema> map = new HashMap<String, Schema>();

    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     *
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public Zero() throws JsonMappingException, JsonParseException {
        JsonToClassConverter converter = new JsonToClassConverter();
        this.map = converter.construct("org.wikimedia.analytics.kraken.schemas.MccMnc", "mcc_mnc.json", "getMCC_MNC");
    }


    /** {@inheritDoc} */
    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.get(0) == null) {
            return null;
        }

        String key = (String) input.get(0);
        MccMnc carrier = (MccMnc) this.map.get(key);
        Tuple output = tupleFactory.newTuple(2);

        if (carrier != null) {
            output.set(0, carrier.getName());
            output.set(1, carrier.getISO());
        } else {
            warn("Key was not found in MccMnc Map", PigWarning.UDF_WARNING_1);
            output.set(0, "no carrier");
            output.set(1, "no country");
        }
        return output;
    }
}
