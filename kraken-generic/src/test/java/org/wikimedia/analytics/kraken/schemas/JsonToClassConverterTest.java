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
}
