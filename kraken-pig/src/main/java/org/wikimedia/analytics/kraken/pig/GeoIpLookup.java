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
import com.maxmind.geoip.LookupService;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.wikimedia.analytics.kraken.schemas.Country;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides a customizable Pig UDF to lookup geographic information belonging
 * to either an IP4 or IP6 address.
 */
public class GeoIpLookup extends EvalFunc<Tuple> {

    private static final String EMPTY_STRING = "";
    private String db;

    private LookupService ip4Lookup;
    private LookupService ip6Lookup;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private PigContext pigContext = new PigContext();

    private HashMap<String, Schema> countries = new HashMap<String, Schema>();

    private final List<GeoIpLookupField> neededGeoFieldNames = new ArrayList<GeoIpLookupField>();
    private final Map<String, GeoIpLookupField> locationFieldMap = new HashMap<String, GeoIpLookupField>();
    private final HashMap<String, String> continentFixes  = new HashMap<String, String>();
    private final HashMap<String, String> continentNameFixes  = new HashMap<String, String>();

    private static final Pattern ip4Pattern = Pattern.compile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$");
    private static final Pattern ip6Pattern = Pattern.compile("^(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}(?::(?:[0-9]{1,3}\\.){3}[0-9]{1,3})*$");

    /**
     * @param inputFields a comma separated list of fields that are requested.
     * Valid field names include: continent, country, city, longitude, latitude, region, state, (only for North America).
     * @param db the name of the Maxmind db to use. Valid choices are: GeoIPCity, GeoIP and GeoIPRegion.
     * We recommend GeoIPCity as that seems to have the most detailed information
     * @throws IOException
     */
    public GeoIpLookup(final String inputFields, final String db) throws IOException{
        init(inputFields, db);
    }

    /**
     *
     * @param inputFields
     * @param db
     * @param execType
     * @throws IOException
     */
    public GeoIpLookup(final String inputFields, final String db, final ExecType execType) throws IOException {
        this.pigContext.setExecType(execType);
        init(inputFields, db);
    }

    /**
     *
     * @param inputFields
     * @param db
     * @throws IOException
     */
    private void init(final String inputFields, final String db) throws IOException {
        this.db = db;

        locationFieldMap.put("countryCode", GeoIpLookupField.COUNTRYCODE);
        locationFieldMap.put("continentName", GeoIpLookupField.CONTINENTNAME);
        locationFieldMap.put("continentCode", GeoIpLookupField.CONTINENTCODE);
        locationFieldMap.put("region", GeoIpLookupField.REGION);
        locationFieldMap.put("city", GeoIpLookupField.CITY);
        locationFieldMap.put("postalCode", GeoIpLookupField.POSTALCODE);
        locationFieldMap.put("latitude", GeoIpLookupField.LATITUDE);
        locationFieldMap.put("longitude", GeoIpLookupField.LONGITUDE);
        locationFieldMap.put("dma_code", GeoIpLookupField.DMACODE);
        locationFieldMap.put("area_code", GeoIpLookupField.AREACODE);
        locationFieldMap.put("metro_code", GeoIpLookupField.METROCODE);

        // A custom function to add continentCode and continentName support, this is not natively offered by the
        // Maxmind database. We load a json file containing the mapping of countries to continents and add two
        // getters.

        JsonToClassConverter converter = new JsonToClassConverter();
        this.countries = converter.construct("org.wikimedia.analytics.kraken.schemas.Country", "country-codes.json", "getA2");

        continentFixes.put("EU", "EU");   // Europe
        continentFixes.put("AP", "AS");   // Asia/Pacific (These addresses often map to Chinese lat/lon, so we choose Asia as the continent.)
        continentFixes.put("A1", "--");   // Anonymous Proxy
        continentFixes.put("A2", "--");   // Satellite Proxy
        continentFixes.put("O1", "--");   // Other Country

        continentNameFixes.put("--", "Unknown");
        continentNameFixes.put(null, "Unknown");
        continentNameFixes.put(EMPTY_STRING, "Unknown");
        continentNameFixes.put("AS", "Asia");
        continentNameFixes.put("EU", "Europe");

        if (!initializeGeoFieldNames(inputFields)) {
            System.out.println("Valid field names are: continentCode, continentName, " + Arrays.toString(Location.class.getFields()));
            throw new RuntimeException("Invalid arguments for GeoIpLookup constructor");
        }
    }

    /**
     * This function checks whether the list of supplied fields are all valid Maxmind database fields.
     *
     * @param inputFields a comma separated list of the required geo fields.
     * @return true/false
     */
    private boolean initializeGeoFieldNames(final String inputFields) {
        Set<String> validGeoFields = this.locationFieldMap.keySet();
        String[] fields = inputFields.split(",");
        for (String field : fields) {
            if (!validGeoFields.contains(field.toString().trim())) {
                return false;
            } else{
                this.neededGeoFieldNames.add(this.locationFieldMap.get(field.toString().trim()));
            }
        }
        return true;
    }


    /**
     *
     * @param countryCode the A2 countryCode that is used as lookup value for continent.
     * @return String
     * @throws ExecException
     */
    private String getContinentName(final String countryCode) throws ExecException{
        if (countryCode != null) {
            Country country = (Country) this.countries.get(countryCode);
            String continentCode = continentFixes.containsKey(countryCode) ? continentFixes.get(countryCode) : country.getContinentCode();
            return continentNameFixes.containsKey(continentCode) ? continentNameFixes.get(continentCode) : country.getContinentName();
        } else {
            return "Unknown";
        }
    }

    /**
     *
     * @param countryCode
     * @return String with continentCode
     * @throws ExecException
     */
    private String getContinentCode(final String countryCode) throws ExecException{
        if (countryCode != null) {
            Country country = (Country) this.countries.get(countryCode);
            return continentFixes.containsKey(countryCode) ? continentFixes.get(countryCode) : country.getContinentCode();
        } else {
            return "Unknown";
        }
    }

    /**
     *
     * @throws IOException
     */
    private void initLookupService() throws IOException {
        if (pigContext.getExecType() == ExecType.LOCAL) {
            ip4Lookup = new LookupService("/usr/share/GeoIP/GeoIPCity.dat", LookupService.GEOIP_MEMORY_CACHE);
            ip6Lookup = new LookupService("/usr/share/GeoIP/GeoIPv6.dat", LookupService.GEOIP_MEMORY_CACHE);
        } else {
            // Enabling cache seems to create weird OOM errors, disabled by default.
            ip4Lookup = new LookupService("./GeoIPCity.dat");
            // There seems to be a bug when enabling LookupService.GEOIP_MEMORY_CACHE for the GeoIPv6 database
            // hence it's disabled.
            ip6Lookup = new LookupService("./GeoIPv6.dat");
        }
    }

    /**
     *
     * @param ip
     * @return
     */
    private Integer determineIpAddressType(final String ip) {
        Matcher ip4 = ip4Pattern.matcher(ip);
        if (ip4.matches()) {
            return 4;
        }
        Matcher ip6 = ip6Pattern.matcher(ip);
        if (ip6.matches()) {
            return 6;
        }
        return 0;
    }

    /**
     *
     * @param ip
     * @return Tuple containing the requested the geocoded field
     * @throws ExecException
     */
    private Tuple doGeoLookup(final String ip) throws ExecException {
        Location location = null;
        Integer ipAddressType = determineIpAddressType(ip);
        Tuple output = tupleFactory.newTuple(this.neededGeoFieldNames.size());
        switch (ipAddressType){
            case 4:
                location = ip4Lookup.getLocation(ip);
                break;
            case 6:
                location = ip6Lookup.getLocationV6(ip);
                break;
            case 0:
                //Not an IP4 or IP6 address
                warn("Supplied variable does not seem to be a valid IP4 or IP6 address.", PigWarning.UDF_WARNING_1);
                return null;
            default:
                //Not an IP4 or IP6 address
                warn("Supplied variable does not seem to be a valid IP4 or IP6 address.", PigWarning.UDF_WARNING_1);
                return null;
        }

        if (location != null) {
            int i = 0;
            String value = null;
            for (GeoIpLookupField field : this.neededGeoFieldNames) {

                switch (field) {
                    case COUNTRYCODE:
                        value = location.countryCode;
                        break;
                    case CONTINENTCODE:
                        value = getContinentCode(location.countryCode);
                        break;
                    case CONTINENTNAME:
                        value =  getContinentName(location.countryCode);
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
        if (input == null || input.size() == 0) {
            return null;
        }

        if (ip4Lookup == null)  {
            initLookupService();
        }

        String ip = (String) input.get(0);
        Tuple output = doGeoLookup(ip);
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
