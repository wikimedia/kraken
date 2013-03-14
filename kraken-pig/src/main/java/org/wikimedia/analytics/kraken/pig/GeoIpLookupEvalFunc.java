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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.wikimedia.analytics.kraken.geo.GeoIpLookup;
import org.wikimedia.analytics.kraken.geo.GeoIpLookupField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides a customizable Pig UDF to lookup geographic information belonging
 * to either an IP4 or IP6 address.
 */
public class GeoIpLookupEvalFunc extends EvalFunc<Tuple> {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private GeoIpLookup geoIpLookup;

    private Map<GeoIpLookupField, Byte> mapping;

    /**
     * @param inputFields a comma separated list of fields that are requested.
     * Valid field names include: continentCode, continentName, country, city, longitude, latitude, region, state, (only for North America).
     * @param db the name of the Maxmind db to use. Valid choices are: GeoIPCity, GeoIP and GeoIPRegion.
     * We recommend GeoIPCity as that seems to have the most detailed information
     * @throws IOException
     */
    public GeoIpLookupEvalFunc(final String inputFields, final String db) throws IOException {
        geoIpLookup = new GeoIpLookup(inputFields, db);

        mapping = new HashMap<GeoIpLookupField, Byte>();
        mapping.put(GeoIpLookupField.COUNTRYCODE, DataType.CHARARRAY);
        mapping.put(GeoIpLookupField.CONTINENTNAME, DataType.CHARARRAY);
        mapping.put(GeoIpLookupField.CONTINENTCODE, DataType.CHARARRAY);
        mapping.put(GeoIpLookupField.REGION, DataType.CHARARRAY);
        mapping.put(GeoIpLookupField.CITY, DataType.CHARARRAY);
        mapping.put(GeoIpLookupField.POSTALCODE, DataType.CHARARRAY);
        mapping.put(GeoIpLookupField.LATITUDE, DataType.FLOAT);
        mapping.put(GeoIpLookupField.LONGITUDE, DataType.FLOAT);
        mapping.put(GeoIpLookupField.DMACODE, DataType.INTEGER);
        mapping.put(GeoIpLookupField.AREACODE, DataType.INTEGER);
        mapping.put(GeoIpLookupField.METROCODE, DataType.INTEGER);
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
            for (GeoIpLookupField field : geoIpLookup.getNeededGeoFieldNames()) {

                switch (field) {
                    case COUNTRYCODE:
                        output.set(i, location.countryCode);
                        break;
                    case CONTINENTCODE:
                        output.set(i, geoIpLookup.getContinentCode(location.countryCode));
                        break;
                    case CONTINENTNAME:
                        output.set(i, geoIpLookup.getContinentName(location.countryCode));
                        break;
                    case REGION:
                        output.set(i, location.region);
                        break;
                    case CITY:
                        output.set(i, location.city);
                        break;
                    case POSTALCODE:
                        output.set(i, location.postalCode);
                        break;
                    case LATITUDE:
                        output.set(i, location.latitude);
                        break;
                    case LONGITUDE:
                        output.set(i, location.longitude);
                        break;
                    case DMACODE:
                        output.set(i, location.dma_code);
                        break;
                    case AREACODE:
                        output.set(i, location.area_code);
                        break;
                    case METROCODE:
                        output.set(i, location.metro_code);
                        break;
                    default:
                        break;
                }
                i++;
            }
        } else {
            warn("MaxMind Geo database does not have location information for the supplied IP address.", PigWarning.UDF_WARNING_3);
            return null;
        }
        return output;
    }

    /**
     *
     * @param ipAddress the ipaddress of the request as recorded by a cache frontend server
     * @param proxyIpRaw a comma-separated list of ip addresses where the left most is the ip address of the originating
     *                   client. See for more details http://en.wikipedia.org/wiki/X-Forwarded-For
     *                   We do this lookup of proxy ip addresses primarily for Opera-powered devices and browsers as they
     *                   all use proxy servers that distort our geocoding efforts.
     * @return
     */
    private String parseIpAddress(final String ipAddress, final String proxyIpRaw) {
        //TODO: we need to make sure that the client ip address in the proxyIpRaw is not an internal address like
        //127.0.0.1 or 192.168.*.*
        if (proxyIpRaw == null || "-".equals(proxyIpRaw)) {
            return ipAddress;
        } else {
            String[] proxyIp = proxyIpRaw.split(",");
            try {
                return proxyIp[0];
            } catch (ArrayIndexOutOfBoundsException e) {
                return null;
            }
        }
    }

    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null) {
            return null;
        }

        Tuple output = tupleFactory.newTuple(geoIpLookup.getNeededGeoFieldNames().size());

        String proxyIp;
        String ip;
        // Check if the proxy ip address has been supplied as well
        try {
            proxyIp = (String) input.get(1);
            ip = parseIpAddress((String) input.get(0), proxyIp);
        } catch (IndexOutOfBoundsException e) {
            // proxy address is not given
            ip = (String) input.get(0);
        }

        if (ip == null) {
            warn("Supplied variable does not seem to be a valid IP4 or IP6 address.", PigWarning.UDF_WARNING_1);
            return null;
        }

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

    /**
     *
     * @param input
     * @return
     */
//    public final Schema outputSchema(final Schema input) {
// this is more complicated
//        // Check that we were passed two fields
//        if (input.size() != 1) {
//            throw new RuntimeException(
//                    "Expected (chararray), input does not have 1 field");
//        }
//
//        try {
//            // Get the types for the column and check them.  If it's
//            // wrong figure out what type was passed and give a good error
//            // message.
//            if (input.getField(0).type != DataType.CHARARRAY) {
//                String msg = "Expected input a list of chararrays, received schema (";
//                msg += DataType.findTypeName(input.getField(0).type);
//                msg += ")";
//                throw new RuntimeException(msg);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
//
//        int i = 0;
//        while (geoIpLookup.getNeededGeoFieldNames().iterator().hasNext()) {
//            GeoIpLookupField field = geoIpLookup.getNeededGeoFieldNames().get()
//            fields.add(new Schema.FieldSchema(null, mapping.get(field)));
//            i++;
//        }
//        return new Schema(fields);
//    }
}
