package org.wikimedia.analytics.kraken.pig;


import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class CountryMetaData {
    public static HashMap<String, Country> constructGeoMetaData() throws JsonParseException, IOException {
        JsonFactory jfactory = new JsonFactory();
        HashMap<String, Country> metadata = new HashMap<String, Country>();
        File file = new File("src/main/resources/country-codes.json");
        JsonParser jParser = jfactory.createJsonParser(file);
        List<Country> countries = new ObjectMapper().readValue(jParser,new TypeReference<List<Country>>() {});

        for(Country country : countries) {
            metadata.put(country.getA2(), country);
        }

        return metadata;

    }

    public static void main(String[] args) throws JsonParseException, IOException {
        HashMap<String, Country> metadata = constructGeoMetaData();
    }
}
