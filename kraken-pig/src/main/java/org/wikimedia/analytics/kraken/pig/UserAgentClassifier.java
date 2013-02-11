/**
 * Copyright (C) 2012-2013  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.pig;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.dclassjni.DclassWrapper;
import org.wikimedia.analytics.kraken.schemas.AppleUserAgent;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserAgentClassifier extends EvalFunc<Tuple> {
    DclassWrapper dw = null;
    private String useragent = null;
    private Map result = new HashMap<String, String>();
    private List args = new ArrayList<String>();
    private final List knownArgs = new ArrayList<String>();

    //Additional Apple device recognizers
    private Pattern AppleBuildIdentifiers = Pattern.compile("(\\d{1,2}[A-L]\\d{1,3}a?)");
    private HashMap<String, Schema> appleProducts = new HashMap<String, Schema>();

    // Wikimedia Mobile Apps regular expressions
    private Pattern Android = Pattern.compile("WikipediaMobile\\/\\d\\.\\d(\\.\\d)?");
    private Pattern Firefox = Pattern.compile(Pattern.quote("Mozilla/5.0%20(Mobile;%20rv:18.0)%20Gecko/18.0%20Firefox/18.0"));
    private Pattern RIM = Pattern.compile(Pattern.quote("Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+"));
    private Pattern Windows = Pattern.compile(Pattern.quote("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)"));

    public Map<String, Pattern> mobileAppPatterns;

    /*
     * UserAgentClassifier constructor that loads:
     * 1) dClass library
     * 2) initializes Wikimedia Mobile app regular expressions
     * 3) load JSON with Apple iOS specific information
     */
    public UserAgentClassifier() throws JsonMappingException, JsonParseException {
        if (this.dw == null) {
            this.dw = new DclassWrapper();
        }
        //knownArgs.add(0,);
        this.dw.initUA();
        mobileAppPatterns = new HashMap<String,Pattern>();
        mobileAppPatterns.put("Wikimedia App Firefox", Firefox);
        mobileAppPatterns.put("Wikimedia App Android", Android);
        mobileAppPatterns.put("Wikimedia App RIM", RIM);
        mobileAppPatterns.put("Wikimedia App Windows", Windows);

        JsonToClassConverter converter = new JsonToClassConverter();
        this.appleProducts = converter.construct("org.wikimedia.analytics.kraken.schemas.AppleUserAgent", "ios.json", "getBuild");

    }

    public UserAgentClassifier(String[] args) throws JsonMappingException, JsonParseException {
        this();
        mobileAppPatterns = new HashMap<String,Pattern>();
    }

    private String unspace(String useragent) {
        return useragent.replace("%20", " ");
    }

    /*
     * If the useragent string is not identified as a mobile device using dClass
     * then we need to determine whether it's an Wikimedia mobile app. This
     * function iterates over a list of regular expressions to look for a match.
     */
    private Tuple detectMobileApp(Tuple output) throws ExecException {
        Pattern pattern;
        boolean foundMatch = false;
        for (Map.Entry<String, Pattern> entry : mobileAppPatterns.entrySet()) {
            pattern = entry.getValue();
            Matcher matcher = pattern.matcher(this.useragent);
            if (matcher.matches()) {
                output.set(5, entry.getKey());
                foundMatch = true;
                break;
            }
        }
        if (!foundMatch) {
            output.set(5, null);
        }
        return output;
    }


    @Override
    /**
     * {@inheritDoc}
     *
     * Method exec takes a {@link Tuple} which should contain a single field, namely the user agent string.
     *
     * returns a tuple with the following fields:
     * 1) Vendor (String)
     * 2) Device OS (String)
     * 3) Device OS version (not for iOS) (String)
     * 4) isWirelessDevice (boolean)
     * 5) isTablet (boolean)
     * 6) Wikimedia Mobile app or null
     * 7) Apple iOS specific information or null
     */
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.get(0) == null) {
            return null;
        }

        String vendor;
        this.useragent = unspace(input.get(0).toString());
        result = this.dw.classifyUA(this.useragent);
        vendor = (String) result.get("vendor");

        //Create the output tuple
        Tuple output = TupleFactory.getInstance().newTuple(6);
        output.set(0, result.get("vendor"));
        output.set(1, result.get("device_os"));
        output.set(2, result.get("device_os_version"));
        output.set(3, convertToBoolean(result, "is_wireless_device"));
        output.set(4, convertToBoolean(result, "is_tablet"));


        if ("generic".equals(vendor)) {
            output = detectMobileApp(output);
        }  else if ("Apple".equals(vendor)) {
            output = postProcessApple(output);
        }


        return output;
    }

    private boolean convertToBoolean(Map<String, String> result, String param){
        return Boolean.parseBoolean(result.get(param));
    }


    /*
     * dClass has identified the mobile device as one from Apple but unfortunately
     * it does not provide reliable iOS version information. This function
     * adds iOS information but care should be used when this data is interpreted:
     * The iOS version is determined using the build number and hence the iOS field
     * should be read as "this mobile device has at least iOS version xyz running".
     */
    private Tuple postProcessApple(Tuple output) throws ExecException{
        Matcher match = AppleBuildIdentifiers.matcher(this.useragent);
        if (match.matches()) {
            String build = match.group(0).toString();
            AppleUserAgent appleUserAgent = (AppleUserAgent) this.appleProducts.get(build);
            output.set(6, appleUserAgent.toString());
        } else {
            output.set(6, null);
        }
        return output;

    }

    private Tuple postProcessSamsung(Tuple output) throws ExecException {
        /*
        This function takes a Samsung model (GT-S5750E, GT S5620) and drops all
        suffix characters and digits to allow for rollup of the keys.
         */

        String model = (String) result.get("model");
        Matcher m = this.Android.matcher(model);
        if (m.matches() && m.groupCount() == 4) {
            String name = m.group(1);
            String value = m.group(3);
            String valueCleaned = value.replaceAll("\\d","");
            String modelCleaned = name + "-" + valueCleaned;
            output.set(2, modelCleaned);
        } else {
            output.set(2, m.group(0));
        }

        return output;

    }

    public void finalize() {
        this.dw.destroyUA();
    }
}
