// Copyright (C) 2013 Wikimedia Foundation
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

package org.wikimedia.analytics.kraken.etl.camus.kafka.coders;

import java.text.DateFormatSymbols;
import java.util.Properties;

import org.wikimedia.analytics.kraken.etl.testutil.LoggingMockingTestCase;

import com.linkedin.camus.coders.CamusWrapper;

public class JsonStringMessageDecoderTest extends LoggingMockingTestCase {
    public void testPropertiesEmpty() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        decoder.init(new Properties(), "someTopic");

        // The default timestamp format uses localized month names :-/ so we
        // have to localize month names to be able to test them.
        String localizedJune = DateFormatSymbols.getInstance().getMonths()[5];

        StringBuilder jsonStringBuilder = new StringBuilder();
        jsonStringBuilder.append("{");
        jsonStringBuilder.append("\"timestamp\": \"[14/");
        jsonStringBuilder.append(localizedJune);
        jsonStringBuilder.append("/2012:17:58:52 +0200]\"");
        jsonStringBuilder.append("}");

        String jsonString = jsonStringBuilder.toString();

        byte[] payload = jsonString.getBytes();

        CamusWrapper<String> res = decoder.decode(payload);

        assertEquals("Parsed time stamp is not 2012-06-14 15:58:52",
                1339689532L * 1000, res.getTimestamp());

        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testPropertiesTimestampField() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.field", "real_timestamp");
        decoder.init(properties, "someTopic");

        // The default timestamp format uses localized month names :-/ so we
        // have to localize month names to be able to test them.
        String localizedJune = DateFormatSymbols.getInstance().getMonths()[5];

        StringBuilder jsonStringBuilder = new StringBuilder();
        jsonStringBuilder.append("{");
        jsonStringBuilder.append("\"timestamp\": \"[13/");
        jsonStringBuilder.append(localizedJune);
        jsonStringBuilder.append("/2012:17:58:52 +0200]\"");
        jsonStringBuilder.append(",");
        jsonStringBuilder.append("\"real_timestamp\": \"[14/");
        jsonStringBuilder.append(localizedJune);
        jsonStringBuilder.append("/2012:17:58:52 +0200]\"");
        jsonStringBuilder.append(",");
        jsonStringBuilder.append("\"timestamp\": \"[15/");
        jsonStringBuilder.append(localizedJune);
        jsonStringBuilder.append("/2012:17:58:52 +0200]\"");
        jsonStringBuilder.append("}");

        String jsonString = jsonStringBuilder.toString();

        byte[] payload = jsonString.getBytes();

        CamusWrapper<String> res = decoder.decode(payload);

        assertEquals("Parsed time stamp is not 2012-06-14 15:58:52",
                1339689532L * 1000, res.getTimestamp());

        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testPropertiesTimestampFormatIsoPlain() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ssZ");
        decoder.init(properties, "someTopic");

        String jsonString = "{\"timestamp\": \"2012-06-14T17:58:52 +0200]\"}";

        byte[] payload = jsonString.getBytes();

        CamusWrapper<String> res = decoder.decode(payload);

        assertEquals("Parsed time stamp is not 2012-06-14 15:58:52",
                1339689532L * 1000, res.getTimestamp());

        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testPropertiesTimestampFormatIsoSubsecond() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        decoder.init(properties, "someTopic");

        String jsonString = "{\"timestamp\": \"2012-06-14T17:58:52.332 +0200]\"}";

        byte[] payload = jsonString.getBytes();

        CamusWrapper<String> res = decoder.decode(payload);

        assertEquals("Parsed time stamp is not 2012-06-14 15:58:52.332",
                1339689532L * 1000 + 332, res.getTimestamp());

        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testPropertiesManyValues() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("someKey", "someValue");
        properties.setProperty("camus.message.timestamp.field", "real_ts");
        properties.setProperty("someOtherKey", "someOtherValue");
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        properties.setProperty("yetAnotherKey", "yetAnotherValue");
        decoder.init(properties, "someTopic");

        String jsonString = "{\"real_ts\": \"2012-06-14T17:58:52.332 +0200]\"}";

        byte[] payload = jsonString.getBytes();

        CamusWrapper<String> res = decoder.decode(payload);

        assertEquals("Parsed time stamp is not 2012-06-14 15:58:52.332",
                1339689532L * 1000 + 332, res.getTimestamp());

        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testJsonWithPayload() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        decoder.init(properties, "someTopic");

        StringBuilder jsonStringBuilder = new StringBuilder();
        jsonStringBuilder.append("{");
        jsonStringBuilder.append("\"payload1\": true,");
        jsonStringBuilder.append("\"payload2\": 42,");
        jsonStringBuilder.append("\"timestamp\": \"2012-06-14T17:58:52.332 +0200]\",");
        jsonStringBuilder.append("\"payload3\": \"foo\",");
        jsonStringBuilder.append("\"payload4\": \"bar\",");
        jsonStringBuilder.append("\"payload5\": [ { \"foo\": \"bar\", \"baz\": 4711 }, 169 ]");
        jsonStringBuilder.append("}");

        String jsonString = jsonStringBuilder.toString();

        byte[] payload = jsonString.getBytes();

        CamusWrapper<String> res = decoder.decode(payload);

        assertEquals("Parsed time stamp is not 2012-06-14 15:58:52.332",
                1339689532L * 1000 + 332, res.getTimestamp());

        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testMalformedJson() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        decoder.init(properties, "someTopic");

        byte[] payload = "{".getBytes();

        try {
            decoder.decode(payload);
        } catch (RuntimeException e) {
            assertLogMessageContains("pars");
        }
    }

    public void testJsonWithoutTimestamp() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        decoder.init(properties, "someTopic");

        String jsonString = "{}";
        byte[] payload = jsonString.getBytes();

        long tsPreDecode = System.currentTimeMillis();
        CamusWrapper<String> res = decoder.decode(payload);
        long tsPostDecode = System.currentTimeMillis();

        assertLogMessageContains("current time");
        assertTrue("Parsed time stamp is before start of decoding",
                tsPreDecode <= res.getTimestamp());
        assertTrue("Parsed time stamp is after end of decoding",
                res.getTimestamp() <= tsPostDecode);
        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testJsonWithMalformedTimestamp() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        decoder.init(properties, "someTopic");

        String jsonString = "{\"timestamp\": \"foo\"}";
        byte[] payload = jsonString.getBytes();

        long tsPreDecode = System.currentTimeMillis();
        CamusWrapper<String> res = decoder.decode(payload);
        long tsPostDecode = System.currentTimeMillis();

        assertLogMessageContains("foo");
        assertLogMessageContains("current time");
        assertTrue("Parsed time stamp is before start of decoding",
                tsPreDecode <= res.getTimestamp());
        assertTrue("Parsed time stamp is after end of decoding",
                res.getTimestamp() <= tsPostDecode);
        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }

    public void testJsonWithTimestampOfWrongType() {
        JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();

        Properties properties = new Properties();
        properties.setProperty("camus.message.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        decoder.init(properties, "someTopic");

        String jsonString = "{\"timestamp\": 4711}";
        byte[] payload = jsonString.getBytes();

        long tsPreDecode = System.currentTimeMillis();
        CamusWrapper<String> res = decoder.decode(payload);
        long tsPostDecode = System.currentTimeMillis();

        assertLogMessageContains("4711");
        assertLogMessageContains("current time");
        assertTrue("Parsed time stamp is before start of decoding",
                tsPreDecode <= res.getTimestamp());
        assertTrue("Parsed time stamp is after end of decoding",
                res.getTimestamp() <= tsPostDecode);
        assertEquals("Parsed record does not match JSON", jsonString, res.getRecord());
    }
}
