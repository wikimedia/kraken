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
/* CityLookupTest.java */

import com.maxmind.geoip.*;
import java.io.IOException;

/* sample of how to use the GeoIP Java API with GeoIP City database */
/* Usage: java CityLookupTest 64.4.4.4 */

/** modified to work with kraken*/

class CityLookupTestV6 {
    public static void main(String[] args) {
        try {
            LookupService cl = new LookupService("/usr/share/GeoIP/GeoIPCityv6.dat",
                    LookupService.GEOIP_MEMORY_CACHE );
            Location l1 = cl.getLocationV6("::213.52.50.8");
            Location l2 = l1;
            System.out.println("countryCode: " + l2.countryCode +
                    "\n countryName: " + l2.countryName +
                    "\n region: " + l2.region +
                    "\n regionName: " + regionName.regionNameByCode(l2.countryCode, l2.region) +
                    "\n city: " + l2.city +
                    "\n postalCode: " + l2.postalCode +
                    "\n latitude: " + l2.latitude +
                    "\n longitude: " + l2.longitude +
                    "\n distance: " + l2.distance(l1) +
                    "\n distance: " + l1.distance(l2) +
                    "\n metro code: " + l2.metro_code +
                    "\n area code: " + l2.area_code +
                    "\n timezone: " + timeZone.timeZoneByCountryAndRegion(l2.countryCode, l2.region));

            cl.close();
        }
        catch (IOException e) {
            System.out.println("IO Exception");
        }
    }
}
