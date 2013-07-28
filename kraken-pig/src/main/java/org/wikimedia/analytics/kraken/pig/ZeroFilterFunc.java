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

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.wikimedia.analytics.kraken.pageview.ProjectInfo;
import org.wikimedia.analytics.kraken.zero.ZeroConfig;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

/**
 * Check whether or not a tuple constitutes a free Wikipedia Zero page view.
 */
public class ZeroFilterFunc extends FilterFunc {

    private String xCS = null;

    private HashMap<String, ZeroConfig> config;

    private HashMap<String, String> xCSCarrierMap;

    private FilterFunc pageViewFilterFunc;

    /**
     *
     */
    public ZeroFilterFunc() {
        this.config = new HashMap<String, ZeroConfig>();

        // Information is obtained from https://wikimediafoundation.org/wiki/Mobile_partnerships
        // see also the Zero namespace on meta: http://meta.wikimedia.org/wiki/Zero:410-01 for example
        config.put("250-99/Beeline/Vimpelcom Beeline", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, true, new String[] {"en", "ru", ""}, null));
        config.put("297-01/Telenor Montenegro/Telenor Montenegro", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, true, new String[] {}, null));
        config.put("404-01/Aircel/Aircel India", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 25, 4, 58), true, true, new String[] {}, null));
        config.put("410-01/Mobilink/Vimpelcom Mobilink Pakistan", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, true, new String[] {"en", "ur", ""}, null));
        config.put("413-02/Dialog Sri Lanka/Dialog Sri Lanka", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {"en", "si", "simple", "ta", ""}, null));
        config.put("420-01/Saudi Telecom STC/Saudi Telecom", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, true, new String[] {"ar", "bn", "en", "tl", "ur", ""}, null));
        config.put("456-02/Hello Cambodia/Hello Cambodia", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {}, null));
        config.put("470-01/Grameenphone Bangladesh/Grameenphone Bangladesh", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {"bn", ""}, null));
        config.put("502-13/Celcom Malaysia/Celcom Malaysia", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {}, null));
        config.put("502-16/Digi/DIGI Malaysia", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {}, null));
        config.put("510-11/XL Axiata/XL Axiata Indonesia", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {"ar", "en", "hi", "id", "jv", "ms", "su", "zh", ""}, null));
        config.put("520-18/DTAC Thailand/DTAC", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {}, null));
        config.put("605-01/Orange/Orange Tunisia", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {"ar", "de", "en", "es", "fr", "it", "ja", "ru", "zh", ""}, null));
        config.put("612-03/Orange/Orange Ivory Coast", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {}, null));
        config.put("614-04/Orange/Orange Niger", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {}, null));
        config.put("624-02/Orange Cameroon/Orange Cameroon", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {"ar", "de", "en", "eo", "es", "fr", "ha", "ln", "yo", "zh", ""}, null));
        config.put("630-86/Orange Congo/Orange Congo", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {}, null));
        config.put("639-07/Orange/Orange Kenya", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {}, null));
        config.put("641-14/Orange/Orange Uganda", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {"ar", "de", "en", "es", "fr", "hi", "ko", "rw", "sw", "zh", ""}, null));
        config.put("652-02/Orange/Orange Botswana", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), true, false, new String[] {}, null));
        config.put("WMF/Wikimedia/Test Configuration", new ZeroConfig("Uganda", "Orange", createStartDate(2013, 6, 1), false, true, new String[] {"en", ""}, null));

        // add a default setting in case we cannot find a carrier
        config.put("default", new ZeroConfig("default", "default", createStartDate(2000, 0, 1), true, true, new String[] {}, null));

        this.xCSCarrierMap = new HashMap<String, String>();
        xCSCarrierMap.put("250-99", "250-99/Beeline/Vimpelcom Beeline");
        xCSCarrierMap.put("297-01", "297-01/Telenor Montenegro/Telenor Montenegro");
        xCSCarrierMap.put("404-01", "404-01/Aircel/Aircel India");
        xCSCarrierMap.put("410-01", "410-01/Mobilink/Vimpelcom Mobilink Pakistan");
        xCSCarrierMap.put("413-02", "413-02/Dialog Sri Lanka/Dialog Sri Lanka");
        xCSCarrierMap.put("420-01", "420-01/Saudi Telecom STC/Saudi Telecom");
        xCSCarrierMap.put("456-02", "456-02/Hello Cambodia/Hello Cambodia");
        xCSCarrierMap.put("470-01", "470-01/Grameenphone Bangladesh/Grameenphone Bangladesh");
        xCSCarrierMap.put("502-13", "502-13/Celcom Malaysia/Celcom Malaysia");
        xCSCarrierMap.put("502-16", "502-16/Digi/DIGI Malaysia");
        xCSCarrierMap.put("510-11", "510-11/XL Axiata/XL Axiata Indonesia");
        xCSCarrierMap.put("520-18", "520-18/DTAC Thailand/DTAC");
        xCSCarrierMap.put("605-01", "605-01/Orange/Orange Tunisia");
        xCSCarrierMap.put("612-03", "612-03/Orange/Orange Ivory Coast");
        xCSCarrierMap.put("614-04", "614-04/Orange/Orange Niger");
        xCSCarrierMap.put("624-02", "624-02/Orange Cameroon/Orange Cameroon");
        xCSCarrierMap.put("630-86", "630-86/Orange Congo/Orange Congo");
        xCSCarrierMap.put("639-07", "639-07/Orange/Orange Kenya");
        xCSCarrierMap.put("641-14", "641-14/Orange/Orange Uganda");
        xCSCarrierMap.put("652-02", "652-02/Orange/Orange Botswana");
        xCSCarrierMap.put("WMF", "WMF/Wikimedia/Test Configuration");

        this.pageViewFilterFunc = new PageViewFilterFunc();
    }

    /**
     *
     * @param input tuple xCS header
     * @return true/false
     * @throws ExecException
     */
    public final Boolean exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null || input.get(8) == null) {
            return false;
        }

        String xCS = (String) input.get(7);
        if (containsXcsValue(xCS)) {
            String carrierName = xCSCarrierMap.get(this.xCS);
            if (carrierName != null) {
                ZeroConfig zeroConfig = getZeroConfig(carrierName);
                return isValidZeroRequest((String) input.get(0), (String) input.get(8), zeroConfig)
                        && pageViewFilterFunc.exec(input);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        byte expectedFieldTypes[] = new byte[] {
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY,
            DataType.CHARARRAY
        };

        if (input == null) {
            throw new RuntimeException("No input schema given");
        }

        if (expectedFieldTypes.length != input.size()) {
            throw new RuntimeException("Expected schema of size "
                    + expectedFieldTypes.length + ", but input schema is of "
                    + "size " + input.size());
        }

        boolean schemaMatches = true;
        for (int idx = 0; idx < expectedFieldTypes.length && schemaMatches; idx++) {
            try {
                schemaMatches &= expectedFieldTypes[idx] == input.getField(idx).type;
            } catch (FrontendException e) {
                throw new RuntimeException("Could not get type for field #" + idx, e);
            }
        }

        if (!schemaMatches) {
            String expectedSchemaStr = "";
            String actualSchemaStr = "";
            for (int idx = 0; idx < expectedFieldTypes.length; idx++) {
                byte expectedFieldType = expectedFieldTypes[idx];
                byte actualFieldType;
                try {
                    actualFieldType = input.getField(idx).type;
                } catch (FrontendException e) {
                    throw new RuntimeException("Could not get type for field #" + idx, e);
                }
                if (idx > 0) {
                    expectedSchemaStr += ", ";
                    actualSchemaStr += ", ";
                }
                expectedSchemaStr += DataType.findTypeName(expectedFieldType);
                actualSchemaStr += DataType.findTypeName(actualFieldType);
            }
            throw new RuntimeException("Input schema (" + actualSchemaStr
                    + ") does not match the expected schema ("
                    + expectedSchemaStr + ")");
        }

        // output is boolean
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }

    /**
     *
     * @param rawString
     * @return
     */
    private boolean containsXcsValue(final String rawString) {
        if (rawString == null) {
            return false;
        }

        // The X-CS header has become the X-Analytics header and is a string
        // of key/value pairs separated by a semi-colon. Iterate over all
        // the pairs to determine whether a key named 'zero' exists. If this
        // is the case then set the variable xCS to that value and return true.
        String[] kvpairs = rawString.split(";");
        for (String kvpair : kvpairs) {
            if (kvpair.startsWith("zero")) {
                this.xCS =  kvpair.substring(5, kvpair.length());
                return true;
            }
        }

        // Initially the X-CS header was just that, a value to indicate
        // which mobile carrier an ip address belongs to. No fancy processing
        // was required.
        if (rawString.matches("\\d{3}.*")) {
            String[] split = rawString.split(";");
            this.xCS = split[0];
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * @param requestedURL
     * @param zeroConfig
     * @return
     */
    private boolean isValidZeroRequest(final String requestedURL, String time, final ZeroConfig zeroConfig) {
        try {
            boolean ret = true;
            URL url = new URL(requestedURL);
            if (ret) {
                ret &= hasValidSubDomain(url, zeroConfig);
            }
            if (ret) {
                ret &= hasValidLanguage(url, zeroConfig);
            }
            if (ret) {
                if (time != null && time.length() > 0) {
                    Date requestDate = null;
                    SimpleDateFormat format;
                    format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                    format.setTimeZone(TimeZone.getTimeZone("GMT"));
                    try {
                        requestDate = format.parse(time);
                    } catch (ParseException e) {
                        format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS");
                        format.setTimeZone(TimeZone.getTimeZone("GMT"));
                        try {
                            requestDate = format.parse(time);
                        } catch (ParseException e2) {
                            format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
                            format.setTimeZone(TimeZone.getTimeZone("GMT"));
                            try {
                                requestDate = format.parse(time);
                            } catch (ParseException e3) {
                                format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                                format.setTimeZone(TimeZone.getTimeZone("GMT"));
                                try {
                                    requestDate = format.parse(time);
                                } catch (ParseException e4) {
                                    System.err.print("Cannot parse date " + time);
                                    ret = false;
                                }
                            }
                        }
                    }
                    if (ret) {
                        ret &= zeroConfig.getStartDate().getTime().before(requestDate);
                    }
                } else {
                    ret = false;
                }
            }
            return ret;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    /**
     *
     * @param url
     * @param zeroConfig
     * @return
     */
    private boolean hasValidSubDomain(final URL url, final ZeroConfig zeroConfig) {
        ProjectInfo projectInfo = new ProjectInfo(url.getHost());

        boolean isMobile = projectInfo.getSiteVersion().equals("M");
        boolean isZero = projectInfo.getSiteVersion().equals("Z");

        if (zeroConfig.isMobileDomainFree() && zeroConfig.isZeroDomainFree()) {
            return (isMobile || isZero);
        } else {
            return (zeroConfig.isMobileDomainFree() && isMobile)
                    || (zeroConfig.isZeroDomainFree() && isZero);
        }
    }

    /**
     *
     * @param url
     * @param zeroConfig
     * @return
     */
    private boolean hasValidLanguage(final URL url, final ZeroConfig zeroConfig) {
        ProjectInfo proj = new ProjectInfo(url.getHost());
        if (zeroConfig.getLanguages().length == 0) {
            return true;
        } else {
            return Arrays.asList(zeroConfig.getLanguages()).contains(proj.getLanguage());
        }
    }

    /**
     *
     * @param year
     * @param month
     * @param day
     * @return
     */
    private Calendar createStartDate(final int year, final int month, final int day) {
        Calendar cal = Calendar.getInstance();
        cal.set(year, month, day);
        return cal;
    }

    private Calendar createStartDate(final int year, final int month, final int day, int hour, int minutes) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.set(year, month, day, hour, minutes);
        return cal;
    }

    /**
     *
     * @param key
     * @return
     */
    public final ZeroConfig getZeroConfig(final String key) {
        return ((key != null) && config.containsKey(key)) ? config.get(key) : config.get("default");
    }

    @Override
    public void finish() {
        // cleaning up delegation filter
        pageViewFilterFunc.finish();
    }
}
