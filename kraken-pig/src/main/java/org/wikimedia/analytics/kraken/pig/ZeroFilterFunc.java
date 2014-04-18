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
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;

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
        config.put("zero-orange-uganda", new ZeroConfig("Uganda", "Orange", createStartDate(2012, 3, 12), true, false, new String[] {"en", "fr", "ko", "de", "zh", "sw", "rw", "ar", "hi", "es", ""}, null));
        config.put("zero-orange-tunesia", new ZeroConfig("Tunisia", "Orange", createStartDate(2012, 3, 24), true, false, new String[] {"ar", "en", "fr", "es", "de", "it", "ru", "jp", "zh", ""}, null));
        config.put("zero-digi-malaysia", new ZeroConfig("Malaysia", "Digi", createStartDate(2012, 4, 21), false, true, new String[] {}, new String[] {"opera"}));
        config.put("zero-orange-niger", new ZeroConfig("Niger", "Orange", createStartDate(2012, 6, 2), true, false, new String[] {}, null));
        config.put("zero-orange-kenya", new ZeroConfig("Kenya", "Orange", createStartDate(2012, 6, 26), true, false, new String[] {}, null));
        config.put("zero-telenor-montenegro", new ZeroConfig("Montenegro", "Telenor", createStartDate(2012, 7, 10), true, true, new String[] {"en", "ru", ""}, null));
        config.put("zero-orange-cameroon", new ZeroConfig("Cameroon", "Orange", createStartDate(2012, 7, 16), true, false, new String[] {"fr", "en", "es", "de", "zh", "ar", "ha", "ln", "yo", "eo", ""}, null));
        config.put("zero-orange-ivory-coast", new ZeroConfig("Ivory Coast", "Orange", createStartDate(2012, 8, 28), true, false, new String[] {}, null));
        config.put("zero-dtac-thailand", new ZeroConfig("Thailand", "dtac", createStartDate(2012, 9, 11), false, true, new String[] {}, null));
        config.put("zero-saudi-telecom", new ZeroConfig("Saudi Arabia", "STC", createStartDate(2012, 9, 14), true, true, new String[] {"ar", "bn", "en", "tl", "ur", ""}, null));
        config.put("zero-orange-congo", new ZeroConfig("Democratic Republic of Congo", "Orange", createStartDate(2012, 11, 6), true, false, new String[] {}, null));
        config.put("zero-orange-botswana", new ZeroConfig("Botswana", "Orange", createStartDate(2013, 1, 8), true, false, new String[] {}, null));
        config.put("zero-beeline-russia", new ZeroConfig("Russia", "Beeline", createStartDate(2013, 2, 28), true, true, new String[] {"ru", "en", ""}, null));
        config.put("zero-xl-axiata-indonesia", new ZeroConfig("Indonesia", "XL Axiata", createStartDate(2013, 3, 1), false, true, new String[] {"id", "en", "zh", "ar", "hi", "ms", "jv", "su", ""}, null));
        config.put("zero-mobilink-pakistan", new ZeroConfig("Pakistan", "Mobilink", createStartDate(2013, 4, 31), true, true, new String[] {"en", "ur", ""}, null));
        config.put("zero-dialog-sri-lanka", new ZeroConfig("Sri Lanka", "Dialog", createStartDate(2000, 1, 1), false, true, new String[] {"en", "ta", "si", ""}, null));
        config.put("zero-hello-cambodia", new ZeroConfig("Cambodia", "Hello", createStartDate(2000, 1, 1), false, true, new String[] {}, null));
        config.put("zero-celcom-malaysia", new ZeroConfig("Malaysia", "Celcom", createStartDate(2000, 1, 1), false, true, new String[] {}, null));
        config.put("zero-tata-india", new ZeroConfig("India", "TATA", createStartDate(2000, 1, 1), false, true, new String[] {}, null));
        config.put("zero-grameenphone-bangladesh", new ZeroConfig("Bangladesh", "Grameenphone", createStartDate(2000, 1, 1), false, true, new String[] {"bn", ""}, null));
        config.put("zero-orange-morocco", new ZeroConfig("Morocco", "Orange Meditel", createStartDate(2000, 1, 1), true, false, new String[] {"fr", "ar", "en", "es", "de", "it", "nl", "pt", "ru", "zh", ""}, null));
        config.put("zero-orange-central-african-republic", new ZeroConfig("Central African Republic", "Orange", createStartDate(2000, 1, 1), true, false, new String[] {"fr", "ar", "sg", "en", "es", "zh", "ha", "ln", "eo", ""}, null));
        config.put("zero-aircel-india", new ZeroConfig("India", "Aircel", createStartDate(2013, 6, 1), true, true, new String[] {"en", "hi", "ta"}, null));

        // add a default setting in case we cannot find a carrier
        config.put("default", new ZeroConfig("default", "default", createStartDate(2000, 0, 1), true, true, new String[] {}, null));

        this.xCSCarrierMap = new HashMap<String, String>();
        xCSCarrierMap.put("641-14", "zero-orange-uganda");
        xCSCarrierMap.put("605-01", "zero-orange-tunesia");
        xCSCarrierMap.put("502-16", "zero-digi-malaysia");
        xCSCarrierMap.put("614-04", "zero-orange-niger");
        xCSCarrierMap.put("639-07", "zero-orange-kenya");
        xCSCarrierMap.put("297-01", "zero-telenor-montenegro");
        xCSCarrierMap.put("624-02", "zero-orange-cameroon");
        xCSCarrierMap.put("612-03", "zero-orange-ivory-coast");
        xCSCarrierMap.put("520-18", "zero-dtac-thailand");
        xCSCarrierMap.put("420-01", "zero-saudi-telecom");
        xCSCarrierMap.put("630-86", "zero-orange-congo");
        xCSCarrierMap.put("652-02", "zero-orange-botswana");
        xCSCarrierMap.put("250-99", "zero-beeline-russia");
        xCSCarrierMap.put("510-11", "zero-xl-axiata-indonesia");
        xCSCarrierMap.put("410-01", "zero-mobilink-pakistan");
        xCSCarrierMap.put("456-02", "zero-hello-cambodia"); // Hello Cambodia
        xCSCarrierMap.put("502-13", "zero-celcom-malaysia"); // Celcom Malaysia
        xCSCarrierMap.put("413-02", "zero-dialog-sri-lanka"); // Dialog Sri Lanka
        xCSCarrierMap.put("405-0", "zero-tata-india");  // Tata
        xCSCarrierMap.put("405-0%2A", "zero-tata-india"); // Tata
        xCSCarrierMap.put("405-0*", "zero-tata-india"); // Tata
        xCSCarrierMap.put("470-01", "zero-grameenphone-bangladesh"); // Grameenphone Bangladesh
        xCSCarrierMap.put("604-00", "zero-orange-morocco"); // Orange Meditel Morocco
        xCSCarrierMap.put("623-03", "zero-orange-central-african-republic"); // Orange Central African Republic
        // Carriers without specific filtering logic
        xCSCarrierMap.put("639-02", "default"); // Safaricom Kenya

        this.pageViewFilterFunc = new PageViewFilterFunc();
    }

    /**
     *
     * @param input tuple xCS header
     * @return true/false
     * @throws ExecException
     */
    public final Boolean exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null) {
            return false;
        }

        String xCS = (String) input.get(7);
        if (containsXcsValue(xCS)) {
            String carrierName = xCSCarrierMap.get(this.xCS);
            ZeroConfig zeroConfig = carrierName != null ? getZeroConfig(carrierName) : getZeroConfig("default");
            return isValidZeroRequest((String) input.get(0), zeroConfig)
                    && pageViewFilterFunc.exec(input);
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
            this.xCS = rawString;
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
    private boolean isValidZeroRequest(final String requestedURL, final ZeroConfig zeroConfig) {
        try {
            URL url = new URL(requestedURL);
            return hasValidSubDomain(url, zeroConfig)
                    && hasValidLanguage(url, zeroConfig);
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
