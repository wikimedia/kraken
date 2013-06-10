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
    private HashMap<String, ZeroConfig> config;

    /**
     *
     */
    public ZeroFilterFunc() {
        this.mode = "default";
        this.config = new HashMap<String, ZeroConfig>();

        config.put("zero-orange-uganda", new ZeroConfig("Uganda", "Orange", createStartDate(2012, 3, 12), true, false, new String[]{"en", "fr", "ko", "de", "zh", "sw", "rw", "ar", "hi", "es"}, null));
        config.put("zero-orange-tunesia", new ZeroConfig("Tunisia", "Orange", createStartDate(2012, 3, 24), true, false, new String[]{"ar", "en", "fr", "es", "de", "it", "ru", "jp", "zh"}, null));
        config.put("zero-digi-malaysia", new ZeroConfig("Malaysia", "Digi", createStartDate(2012, 4, 21), false, true, new String[]{}, new String[]{"opera"}));
        config.put("zero-orange-niger", new ZeroConfig("Niger", "Orange", createStartDate(2012, 6, 2), true, false, new String[]{}, null));
        config.put("zero-orange-kenya", new ZeroConfig("Kenya", "Orange", createStartDate(2012, 6, 26), true, false, new String[]{}, null));
        config.put("zero-telenor-montenegro", new ZeroConfig("Montenegro", "Telenor", createStartDate(2012, 7, 10), true, true, new String[]{}, null));
        config.put("zero-orange-cameroon", new ZeroConfig("Cameroon", "Orange", createStartDate(2012, 7, 16), true, false, new String[]{"fr", "en", "es", "de", "zh", "ar", "ha", "ln", "yo", "eo"}, null));
        config.put("zero-orange-ivory-coast", new ZeroConfig("Ivory Coast", "Orange", createStartDate(2012, 8, 28), true, false, new String[]{}, null));
        config.put("zero-dtac-thailand", new ZeroConfig("Thailand", "dtac", createStartDate(2012, 9, 11), false, true, new String[]{}, null));
        config.put("zero-saudi-telecom", new ZeroConfig("Saudi Arabia", "STC", createStartDate(2012, 9, 14), true, true, new String[]{}, null));
        config.put("zero-orange-congo", new ZeroConfig("Democractic Republic of Congo", "Orange", createStartDate(2012, 11, 6), true, false, new String[]{}, null));
        config.put("zero-orange-botswana", new ZeroConfig("Botswana", "Orange", createStartDate(2013, 1, 8), true, false, new String[]{}, null));
        config.put("zero-beeline-russia", new ZeroConfig("Russia", "Beeline", createStartDate(2013, 2, 28), true, true, new String[]{"ru", "en"}, null));
        config.put("zero-xl-axiata-indonesia", new ZeroConfig("Indonesia", "XL Axiata", createStartDate(2013, 3, 1), false, true, new String[] {"id", "en", "zh", "ar", "hi", "ms", "jv", "su"}, null));
        config.put("zero-mobilink-pakistan", new ZeroConfig("Pakistan", "Mobilink", createStartDate(2013, 4, 31), true, true, new String[] {"en", "ur"}, null));
        // add a default setting in case we cannot find a carrier
        config.put("default", new ZeroConfig("default", "default", createStartDate(1990, 0, 1), true, true, new String[] {}, null));
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
            String xCS = (String) input.get(0);
            return containsXcsValue(xCS);
        } else {
            String simpleFilename = simplifyFilename((String) input.get(1));
            ZeroConfig zeroConfig = getZeroConfig(simpleFilename);
            return isLegacyZeroRequest((String) input.get(0), zeroConfig);
        }
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        if (mode.equals("default")) {
            if (input.size() != 1) {
                throw new RuntimeException(
                    "Expected url (chararray), input should be exactly 1 field.");
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
        String[] kvpairs = rawString.split(";");
        for (String kvpair : kvpairs) {
            if (kvpair.startsWith("zero")) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param requestedURL
     * @param zeroConfig
     * @return
     */
    private boolean isLegacyZeroRequest(final String requestedURL, final ZeroConfig zeroConfig) {
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
        if (zeroConfig.isMobileDomainFree() && zeroConfig.isZeroDomainFree()) {
           return url.getHost().contains(".m.") || url.getHost().contains(".zero.");
        } else {
            return (zeroConfig.isMobileDomainFree() && url.getHost().contains(".m."))
            || (zeroConfig.isZeroDomainFree() && url.getHost().contains(".zero."));
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
        if (zeroConfig.getLanguages().length == 0 ) {
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
        return filename != null ? filename.substring(0, filename.indexOf(".")) : "";
    }

    /**
     *
     * @param filename
     * @return
     */
    public final ZeroConfig getZeroConfig(final String filename) {
        return ((filename != null) && config.containsKey(filename)) ? config.get(filename) : config.get("default");
    }
}
