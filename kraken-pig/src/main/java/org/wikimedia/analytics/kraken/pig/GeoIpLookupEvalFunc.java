/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
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

 */

package org.wikimedia.analytics.kraken.pig;

import com.maxmind.geoip.Location;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.geo.GeoIpLookup;
import org.wikimedia.analytics.kraken.geo.GeoIpLookupField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides a customizable Pig UDF to lookup geographic information belonging
 * to either an IP4 or IP6 address.
 */
public class GeoIpLookupEvalFunc extends EvalFunc<Tuple> {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private GeoIpLookup geoIpLookup;

    /**
     * @param inputFields a comma separated list of fields that are requested.
     * Valid field names include: continentCode, continentName, country, city, longitude, latitude, region, state, (only for North America).
     * @param db the name of the Maxmind db to use. Valid choices are: GeoIPCity, GeoIP and GeoIPRegion.
     * We recommend GeoIPCity as that seems to have the most detailed information
     * @throws IOException
     */
    public GeoIpLookupEvalFunc(final String inputFields, final String db) throws IOException {
        geoIpLookup = new GeoIpLookup(inputFields, db);
    }


    /**
     *
     * @param inputFields
     * @param db
     * @param execType
     * @throws IOException
     */
    public GeoIpLookupEvalFunc(final String inputFields, final String db, final String execType) throws IOException {
        geoIpLookup = new GeoIpLookup(inputFields, db, execType);
    }
    /**
     *
     * @param location instance of the Maxmind Location class
     * @param output instance of a Pig Tuple class
     * @return
     * @throws ExecException
     */
    private Tuple setResult(final Location location, final Tuple output) throws ExecException {
        if (location != null) {
            int i = 0;
            String value = null;
            for (GeoIpLookupField field : geoIpLookup.getNeededGeoFieldNames()) {

                switch (field) {
                    case COUNTRYCODE:
                        value = location.countryCode;
                        break;
                    case CONTINENTCODE:
                        value = geoIpLookup.getContinentCode(location.countryCode);
                        break;
                    case CONTINENTNAME:
                        value = geoIpLookup.getContinentName(location.countryCode);
                        break;
                    case REGION:
                        value = location.region;
                        break;
                    case CITY:
                        value = location.city;
                        break;
                    case POSTALCODE:
                        value = location.postalCode;
                        break;
                    case LATITUDE:
                        value = Float.toString(location.latitude);
                        break;
                    case LONGITUDE:
                        value = Float.toString(location.longitude);
                        break;
                    case DMACODE:
                        value = Integer.toString(location.dma_code);
                        break;
                    case AREACODE:
                        value = Integer.toString(location.area_code);
                        break;
                    case METROCODE:
                        value = Integer.toString(location.metro_code);
                        break;
                    default:
                        break;
                }
                output.set(i, value);
                i++;
            }
        } else {
            warn("MaxMind Geo database does not have location information for the supplied IP address.", PigWarning.UDF_WARNING_3);
            return null;
        }
        return output;
    }

    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }

        Tuple output = tupleFactory.newTuple(geoIpLookup.getNeededGeoFieldNames().size());

        String ip = (String) input.get(0);
        Location location = geoIpLookup.doGeoLookup(ip);
        if (location != null){
            output = setResult(location, output);
        } else {
            warn("Supplied variable does not seem to be a valid IP4 or IP6 address.", PigWarning.UDF_WARNING_1);
            return null;
        }
        return output;
    }

    @Override
    public final List<String> getCacheFiles() {
        List<String> cacheFiles = new ArrayList<String>();
        cacheFiles.add("GeoIPCity.dat#GeoIPCity.dat");
        cacheFiles.add("GeoIP.dat#GeoIP.dat");
        cacheFiles.add("GeoIPRegion.dat#GeoIPRegion.dat");
        cacheFiles.add("GeoIPv6.dat#GeoIPv6.dat");
        return cacheFiles;
    }
}
