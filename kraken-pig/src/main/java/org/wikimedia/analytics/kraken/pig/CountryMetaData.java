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
package org.wikimedia.analytics.kraken.pig;


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


public class CountryMetaData {
    public static HashMap<String, Country> constructGeoMetaData() throws JsonMappingException, JsonParseException {
        JsonFactory jfactory = new JsonFactory();
        HashMap<String, Country> metadata = new HashMap<String, Country>();
        List<Country> countries = null;

        InputStream input =
                CountryMetaData.class.getClassLoader().getResourceAsStream("country-codes.json");
        try {
            JsonParser jParser = jfactory.createJsonParser(input);
            countries = new ObjectMapper().readValue(jParser,new TypeReference<List<Country>>() {});
        } catch (IOException e) {
                System.err.println("Could not load country-codes.json");
        } finally {
            try {
                input.close();
            } catch (IOException ee){
                System.err.println("Could not close filestream");
                }
            }

        for(Country country : countries) {
            metadata.put(country.getA2(), country);
        }
        return metadata;

    }

    public static void main(String[] args) throws JsonParseException, IOException {
        HashMap<String, Country> metadata = constructGeoMetaData();
    }
}
