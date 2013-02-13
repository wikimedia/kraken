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
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.schemas.Country;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides a customizable Pig UDF to lookup geographic information belonging
 * to either an IP4 or IP6 address.
 */

public class GeoIpLookup extends EvalFunc<Tuple> {

    private static final String EMPTY_STRING = "";
    private File dbFullPath;
    private File dbFullip6Path;
    private String db;

    private LookupService ip4Lookup;
    private LookupService ip6Lookup;
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private String dbPath = "/usr/share/GeoIP";

    private HashMap<String, Schema> countries = new HashMap<String, Schema>();
    private HashMap<String, String> databases =  new HashMap<String, String>();

    private final List<String> neededGeoFieldNames = new ArrayList<String>();
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

    /** {@inheritDoc}
     * @param geoDbPath the full path to a Maxmind db on the local filesystem, this is useful if you want to
     * use an different / older databases then the ones that are installed on Kraken.
     * @throws IOException
     */
    public GeoIpLookup(final String inputFields, final String db, final String geoDbPath) throws IOException {
        this.dbPath = geoDbPath;
        init(inputFields, db);
    }

    /**
     *
     * @param inputFields
     * @param db
     * @throws IOException
     * @throws RuntimeException
     */
    private void init(final String inputFields,  final String db) throws IOException, RuntimeException {
        this.db = db;

        // A custom function to add continentCode and continentName support, this is not natively offered by the
        // Maxmind database. We load a json file containing the mapping of countries to contintents and add two
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

        this.databases.put("GeoIPCity", "GeoIPCity.dat");
        this.databases.put("GeoIP", "GeoIP.dat");
        this.databases.put("GeoIPRegion", "GeoIPRegion.dat");
        this.databases.put("GeoIPv6", "GeoIPv6.dat");

        if (!validateGeoFieldNames(inputFields) || (!validateDatabaseName(db))) {
            System.out.println("Valid field names are: continentCode, continentName, " + Arrays.toString(Location.class.getFields()));
            throw new RuntimeException("Invalid arguments for GeoIpLookup constructor");
        }

        if (ip4Lookup == null) {
            this.dbFullPath = new File(this.dbPath, this.databases.get(db));
            ip4Lookup = new LookupService(this.dbFullPath.getPath(), LookupService.GEOIP_MEMORY_CACHE);
        }

        if (ip6Lookup == null) {
            this.dbFullip6Path = new File(this.dbPath, this.databases.get("GeoIPv6"));
            ip6Lookup = new LookupService(this.dbFullip6Path.getPath(), LookupService.GEOIP_MEMORY_CACHE);
        }
    }

    /**
     * This function checks whether the supplied Maxmind database name is recognized or not.
     */
    private boolean validateDatabaseName(final String db) {
        if (!this.databases.containsKey(db)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * This function checks whether the list of supplied fields are all valid Maxmind database fields.
     *
     * @param inputFields
     * @return
     */

    private boolean validateGeoFieldNames(final String inputFields) {
        Field[] validGeoFields = Location.class.getFields();
        String[] fields = inputFields.split(",");
        for (String field : fields) {
            if (field.trim().contains("continent")) {
                this.neededGeoFieldNames.add(field.trim());
            }
            for (Field validField : validGeoFields) {
                if (validField.getName().equals(field.trim().toString())){
                    this.neededGeoFieldNames.add(field.trim());
                    break;
                }
            }
        }
        if (this.neededGeoFieldNames.size() > 0) {
            return true;
        } else {
            return false;
        }

    }

    /**
     *
     * @param countryCode
     * @param output
     * @param i
     * @return
     * @throws ExecException
     */
    private Tuple getContinentName(final String countryCode, Tuple output, final int i) throws ExecException{
        if (countryCode != null) {
            Country country = (Country) this.countries.get(countryCode);
            String continentCode = continentFixes.containsKey(countryCode) == true ? continentFixes.get(countryCode) : country.getContinentCode();
            String continentName = continentNameFixes.containsKey(continentCode) == true ? continentNameFixes.get(continentCode) : country.getContinentName();
            output.set(i, continentName);
        } else {
            output.set(i, "Unknown");

        }
        return output;
    }

    /**
     *
     * @param countryCode
     * @param output
     * @param i
     * @return
     * @throws ExecException
     */
    private Tuple getContinentCode(final String countryCode, Tuple output, int i) throws ExecException{
        if (countryCode != null) {
            Country country = (Country) this.countries.get(countryCode);
            String continentCode = continentFixes.containsKey(countryCode) == true ? continentFixes.get(countryCode) : country.getContinentCode();
            output.set(i, continentCode);
        } else {
            output.set(i, "Unknown");
        }
        return output;
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
     * @return
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
                location = ip6Lookup.getLocation(ip);
                break;
            case 0:
                //Not an IP4 or IP6 address
                warn("Supplied variable does not seem to be a valid IP4 or IP6 address.", PigWarning.UDF_WARNING_1);
                return output;
            default:
                //Not an IP4 or IP6 address
                warn("Supplied variable does not seem to be a valid IP4 or IP6 address.", PigWarning.UDF_WARNING_1);
                return output;
        }


        if (location != null) {
            int i = 0;
            String value;
            for (String field : this.neededGeoFieldNames) {
                try {
                    if (!field.contains("continent")) {
                        value = location.getClass().getField(field).get(location) != null ? location.getClass().getField(field).get(location).toString() : EMPTY_STRING;
                        output.set(i, value);

                    } else if (field.toString().equals("continentCode")) {
                        output = getContinentCode(location.countryCode, output, i);

                    } else if (field.toString().equals("continentName")) {
                        output = getContinentName(location.countryCode, output, i);
                    }
                } catch (NoSuchFieldException e) {
                    warn("Location class does not contain the requested field.", PigWarning.UDF_WARNING_2);
                } catch (IllegalAccessException e) {

                }
                i++;
            }
        } else {
            warn("MaxMind Geo database " + this.dbPath.toString() + " does not have location information for the supplied IP address.", PigWarning.UDF_WARNING_3);
            return null;
        }
        return output;
    }


    /** {@inheritDoc} */
    @Override
    public Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        String ip = (String) input.get(0);
        Tuple output = doGeoLookup(ip);
        return output;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> getCacheFiles() {
        List<String> cacheFiles = new ArrayList<String>(1);
        // Note that this forces us to use basenames only.  If we need
        // to support other paths, we either need two arguments in the
        // constructor, or to parse the filename to extract the basename.
        cacheFiles.add(this.databases.get(this.db) + "#" + this.databases.get(this.db));
        cacheFiles.add(this.databases.get("GeoIPv6") + "#" + this.databases.get("GeoIPv6"));
        return cacheFiles;
    }

}
