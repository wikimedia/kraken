/**
 * Copyright (C) 2013  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
