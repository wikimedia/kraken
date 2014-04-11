/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
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
package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.wikimedia.analytics.kraken.pageview.ProjectInfo;
import org.wikimedia.analytics.kraken.zero.ZeroConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;

/**
 *
 */
public class ZeroFilterFunc extends FilterFunc {

    private String mode = null;

    private String xCS = null;

    private HashMap<String, ZeroConfig> config;

    private HashMap<String, String> xCSCarrierMap;

    /**
     *
     */
    public ZeroFilterFunc() {
        this.mode = "default";

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
    }

    /**
     *
     * @param mode
     */
    public ZeroFilterFunc(final String mode) {
        this();
        if (mode.equals("legacy") || mode.equals("default")) {
            this.mode = mode;
        } else {
            throw new RuntimeException("Expected mode is 'default' or 'legacy'. ");
        }
    }

    /**
     *
     * @param input tuple xCS header
     * @return true/false
     * @throws ExecException
     */
    public final Boolean exec(final Tuple input) throws ExecException {
        if (input == null || input.get(0) == null) {
            return false;
        }

        if (mode.equals("default")) {
            String xCS = (String) input.get(1);
            if (containsXcsValue(xCS)) {
                String carrierName = xCSCarrierMap.get(this.xCS);
                ZeroConfig zeroConfig = carrierName != null ? getZeroConfig(carrierName) : getZeroConfig("default");
                return isValidZeroRequest((String) input.get(0), zeroConfig);
            } else {
                return false;
            }
        } else {
            String simpleFilename = simplifyFilename((String) input.get(1));
            ZeroConfig zeroConfig = getZeroConfig(simpleFilename);
            return isValidZeroRequest((String) input.get(0), zeroConfig);
        }
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        if (mode.equals("default")) {
            if (input.size() != 2) {
                throw new RuntimeException(
                        "Expected url (chararray) and x-cs header (chararray), input should be exactly 2 fields.");
            } else if (mode.equals("legacy")) {
                if (input.size() != 2) {
                    throw new RuntimeException(
                            "Expected url (chararray) and filename (chararray),  input should be exactly 2 fields.");
                }
            }
        }

        try {
            // Get the types for the column and check them.  If it's
            // wrong figure out what type was passed and give a good error
            // message.
            if (input.getField(0).type != DataType.CHARARRAY) {
                String msg = "Expected input chararray, received schema (";
                msg += DataType.findTypeName(input.getField(0).type);
                msg += ")";
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            return url.getPath().contains("/wiki/")
                    && url.getHost().contains("wikipedia")
                    && hasValidSubDomain(url, zeroConfig)
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
     * @return String: the mode in which ZeroFilterFunc is running
     */
    public final String getMode() {
        return mode;
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
     * @param filename
     * @return
     */
    private String simplifyFilename(final String filename) {
        // This will remove all the file extensions and timestamp information from the filename.
        try {
            return filename != null ? filename.substring(0, filename.indexOf(".")) : "";
        } catch (StringIndexOutOfBoundsException e) {
            warn("Could not simplify filename: " + filename, PigWarning.UDF_WARNING_1);
            return filename;
        }
    }

    /**
     *
     * @param key
     * @return
     */
    public final ZeroConfig getZeroConfig(final String key) {
        return ((key != null) && config.containsKey(key)) ? config.get(key) : config.get("default");
    }
}
