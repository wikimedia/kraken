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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;
import org.wikimedia.analytics.kraken.schemas.MccMnc;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ZeroTest {

    @Test
    public void Test1() throws JsonMappingException, JsonParseException {
        Zero zero = new Zero();
        HashMap<String, MccMnc> mcc_mnc = zero.constructMccMncData();
        assertNotNull(mcc_mnc);
    }

    @Test
    public void Test2() throws JsonMappingException, JsonParseException {
        Zero zero = new Zero();
        HashMap<String, MccMnc> mcc_mnc = zero.constructMccMncData();
        MccMnc carrier = mcc_mnc.get("624-02");
        assertNotNull(carrier);
        assertEquals("624-02", carrier.getMCC_MNC());
        assertEquals("orange-cameroon", carrier.getName());
        assertEquals("Cameroon", carrier.getCountry());
    }


}
