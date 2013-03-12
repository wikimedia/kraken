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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.MccMnc;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ZeroTest {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testMccMncJsonFile() throws JsonMappingException, JsonParseException {
        JsonToClassConverter converter = new JsonToClassConverter();
        HashMap<String, Schema> map = converter.construct("org.wikimedia.analytics.kraken.schemas.MccMnc", "mcc_mnc.json", "getMCC_MNC");

        MccMnc carrier = (MccMnc) map.get("624-02");
        assertNotNull(carrier);
        assertEquals("624-02", carrier.getMCC_MNC());
        assertEquals("orange-cameroon", carrier.getName());
        assertEquals("Cameroon", carrier.getCountry());
    }

    @Test
    public void testMccMncJsonFile2() throws JsonMappingException, JsonParseException {
        JsonToClassConverter converter = new JsonToClassConverter();
        HashMap<String, Schema> map = converter.construct("org.wikimedia.analytics.kraken.schemas.MccMnc", "mcc_mnc.json", "getMCC_MNC");
        MccMnc carrier = (MccMnc) map.get("420-01");
        assertNotNull(carrier);
        assertEquals("420-01", carrier.getMCC_MNC());
        assertEquals("stc/al-jawal-saudi-arabia", carrier.getName());
        assertEquals("Saudi Arabia", carrier.getCountry());
    }

    @Test
    public void testZeroWithMFAKey() throws IOException, JsonParseException, JsonMappingException {
        Tuple input = tupleFactory.newTuple(1);
        input.set(0, "zero=420-01;mf-m=a");
        Zero zero = new Zero();
        Tuple carrier = zero.exec(input);
        assertEquals("stc/al-jawal-saudi-arabia", carrier.get(0));
        assertEquals("SA", carrier.get(1));
    }

    @Test
    public void testZeroWithMFAKey2() throws IOException, JsonParseException, JsonMappingException {
        Tuple input = tupleFactory.newTuple(1);
        input.set(0, "zero=420-01");
        Zero zero = new Zero();
        Tuple carrier = zero.exec(input);
        assertEquals("stc/al-jawal-saudi-arabia", carrier.get(0));
        assertEquals("SA", carrier.get(1));
    }

    @Test
    public void testZeroWithoutMFAKey() throws IOException, JsonParseException, JsonMappingException {
        Tuple input = tupleFactory.newTuple(1);
        input.set(0, "420-01");
        Zero zero = new Zero();
        Tuple carrier = zero.exec(input);
        assertEquals("stc/al-jawal-saudi-arabia", carrier.get(0));
        assertEquals("SA", carrier.get(1));
    }
}
