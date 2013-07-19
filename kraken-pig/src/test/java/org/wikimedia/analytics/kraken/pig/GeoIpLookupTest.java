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

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class GeoIpLookupTest {
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private Tuple input = tupleFactory.newTuple(2);

    /**
     * ipAddress
     * @throws IOException
     */
    @Test
    public void testExec1() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode, continentCode, continentName", "GeoIPCity", "LOCAL");
        this.input.set(0, "71.217.23.156");
        Tuple geoData = geo.exec(this.input);
        assertNotNull(geoData);
        System.out.println(geoData.toString());
        String countryCode = (String) geoData.get(0);
        assertEquals("US", countryCode);
        String continentCode = (String) geoData.get(1);
        assertEquals("NA", continentCode);
        String continentName = (String) geoData.get(2);
        assertEquals("North America", continentName);
    }

    /**
     * localhost
     * @throws IOException
     */
    @Test
    public void testExec2() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode, continentCode, continentName", "GeoIP", "LOCAL");
        this.input.set(0, "127.0.0.1");
        Tuple output = geo.exec(this.input);
        assertNull(output);
    }

    /**
     * A1, Anonymous Proxy
     * @throws IOException
     */
    @Test
    public void testExec3() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode, continentCode, continentName", "GeoIPCity","LOCAL");
        this.input.set(0, "65.49.68.181");
        Tuple geoData = geo.exec(this.input);
        assertNotNull(geoData);
        System.out.println(geoData.toString());
        String countryCode = (String) geoData.get(0);
        assertEquals("A1", countryCode);
        String continentCode = (String) geoData.get(1);
        assertEquals("--", continentCode);
        String continentName = (String) geoData.get(2);
        assertEquals("Unknown", continentName);
    }

    /**
     * AP, Asia/Pacific
     * @throws IOException
     */
    @Test
    public void testExec4() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode, continentCode, continentName", "GeoIPCity", "LOCAL");
        this.input.set(0, "206.53.148.17");
        Tuple geoData = geo.exec(this.input);
        assertNotNull(geoData);
        String countryCode = (String) geoData.get(0);
        assertEquals("AP", countryCode);
        String continentCode = (String) geoData.get(1);
        assertEquals("AS", continentCode);
        String continentName = (String) geoData.get(2);
        assertEquals("Asia", continentName);
    }

    /**
     * Wikipedia
     * @throws IOException
     */
    @Test
    public void testExec5() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode", "GeoIPCity", "LOCAL");
        this.input.set(0, "wikipedia.org");
        Tuple output = geo.exec(this.input);
        assertEquals(null, output);
    }

    /**
     * Yemen, was getting bad results for this.
     * @throws IOException
     */
    @Test
    public void testExec6() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode, continentCode, continentName", "GeoIPCity", "LOCAL");
        input.set(0, "185.11.8.188");
        Tuple geoData = geo.exec(input);
        assertNotNull(geoData);
        System.out.println(geoData.toString());
        String countryCode = (String) geoData.get(0);
        assertEquals("YE", countryCode);
        String continentCode = (String) geoData.get(1);
        assertEquals("AS", continentCode);
        String continentName = (String) geoData.get(2);
        assertEquals("Asia", continentName);
    }

    /**
     * test proxyaddress substitution
     * @throws IOException
     */
    @Test
    public void testProxyIp() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode, continentCode, continentName", "GeoIP", "LOCAL");
        this.input.set(0, "37.228.105.17"); // this ip address is an Opera proxy server
        this.input.set(1, "101.209.27.230,%20101.209.27.230"); // these are random ip addresses
        Tuple output = geo.exec(this.input);
        String countryCode = (String) output.get(0);
        assertEquals("IN", countryCode);
    }

    /**
     * Test country code for IPv6 address
     * @throws IOException
     */
    @Test
    public void testIpv6CountryCode() throws IOException {
        GeoIpLookupEvalFunc geo = new GeoIpLookupEvalFunc("countryCode", "GeoIPCity", "LOCAL");
        this.input.set(0, "2001:ab0:abcd:1234::2222"); // Arbitrary IPv6 address in Germany
        Tuple output = geo.exec(this.input);
        assertNotNull("Returned tuple is null", output);
        assertEquals("Returned tuple element cound does not contain match", 1, output.size());
        assertEquals("Returned tuple's value does not match", "DE", output.get(0));
    }
}
