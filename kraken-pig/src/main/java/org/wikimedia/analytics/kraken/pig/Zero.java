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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.MccMnc;
import org.wikimedia.analytics.kraken.schemas.Schema;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;
import java.io.IOException;
import java.util.HashMap;

/**
 * This PIG UDF is used to map the custom X-CS http header to the mobile carrier and country
 * and this is part of the Wikipedia Zero project. This function returns two fields:
 * the carrier name and the ISO-3316 country code.
 */

public class Zero extends EvalFunc<Tuple> {

    HashMap<String, Schema> map = new HashMap<String, Schema>();
    private TupleFactory tupleFactory = TupleFactory.getInstance();


    /**
     *
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public Zero() throws JsonMappingException, JsonParseException{
        JsonToClassConverter converter = new JsonToClassConverter();
        map = converter.construct("org.wikimedia.analytics.kraken.schemas.MccMnc", "mcc_mnc.json", "getCountryCode");
    }


    /** {@inheritDoc} */
    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1) {
            return null;
        }
        String key = (String) input.get(0);
        MccMnc zero = (MccMnc) map.get(key);
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
