/**
 * Copyright (C) 2013  Wikimedia Foundation

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.kraken.geo;


import java.io.IOException;

import junit.framework.TestCase;

import com.maxmind.geoip.Location;


public class GeoIpLookupTest extends TestCase {

    public void testDoGeoLookupIpv6US() throws IOException {
        GeoIpLookup lookup = new GeoIpLookup("countryCode", "GeoIPCity",
                "LOCAL");

        // Arbitrary IPv6 address in the US
        String ip = "2600:1011:b103:9999:6864:ac5e:1686:c20f";

        Location location = lookup.doGeoLookup(ip);

        assertNotNull("location is null", location);
        assertEquals("US", location.countryCode);
        assertEquals("United States", location.countryName);
    }

    public void testDoGeoLookupIpv6Europe() throws IOException {
        GeoIpLookup lookup = new GeoIpLookup("countryCode", "GeoIPCity",
                "LOCAL");

        // Arbitrary IPv6 address in Germany
        String ip = "2001:aa8:abcd:1234::2222";

        Location location = lookup.doGeoLookup(ip);

        assertNotNull("location is null", location);
        assertEquals("DE", location.countryCode);
        assertEquals("Germany", location.countryName);
    }
}
