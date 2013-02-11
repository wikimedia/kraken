/**
 Copyright (C) 2012  Wikimedia Foundation

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.schemas;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.junit.Test;

import java.util.HashMap;

import static junit.framework.Assert.assertEquals;


public class JsonToClassConverterTest {

    @Test
    public void readAppleUserAgentJsonFile()
            throws JsonMappingException, JsonParseException, RuntimeException {
        JsonToClassConverter converter = new JsonToClassConverter();
        HashMap<String, Schema> map = converter.construct("org.wikimedia.analytics.kraken.schemas.AppleUserAgent", "ios.json", "getProduct");
        assertEquals(map.keySet().size(), 89);

    }

    @Test
    public void readMccMncJsonFile()
            throws JsonMappingException, JsonParseException, RuntimeException {
        JsonToClassConverter converter = new JsonToClassConverter();
        HashMap<String, Schema> map = converter.construct("org.wikimedia.analytics.kraken.schemas.MccMnc", "mcc_mnc.json", "getCountryCode");
        assertEquals(map.keySet().size(), 211);

    }


    @Test
    public void readCountryJsonFile()
            throws JsonMappingException, JsonParseException, RuntimeException {
        JsonToClassConverter converter = new JsonToClassConverter();
        HashMap<String, Schema> map = converter.construct("org.wikimedia.analytics.kraken.schemas.Country", "country-codes.json", "getA2");
        assertEquals(map.keySet().size(), 249);

    }
}
