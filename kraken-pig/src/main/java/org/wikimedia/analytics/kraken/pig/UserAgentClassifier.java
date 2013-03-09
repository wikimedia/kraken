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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.wikimedia.analytics.dclassjni.DclassWrapper;
import org.wikimedia.analytics.dclassjni.Result;
import org.wikimedia.analytics.kraken.schemas.AppleUserAgent;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class uses the dClass mobile device user agent decision tree to determine vendor/version of a mobile device.
 * dClass is built on the OpenDDR project.
 *
 * NOTES:
 * 1) iPod devices are treated as if they are iPhones
 * 2) Not all Apple build id's are recognized
 * 3) Browser version is poorly supported by openDDR, especially for Apple devices
 * 4) The Apple post processor function does try to fix of the openDDR issues but it is definitely not perfect.
 */
public class UserAgentClassifier extends EvalFunc<Tuple> {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private DclassWrapper dw = null;
    private String useragent = null;
    private Result result;

    //Additional Apple device recognizers
    private Pattern appleBuildIdentifiers = Pattern.compile("(\\d{1,2}[A-L]\\d{1,3}a?)");
    private HashMap<String, org.wikimedia.analytics.kraken.schemas.Schema> appleProducts;

    // Wikimedia Mobile Apps regular expressions
    private Pattern android = Pattern.compile("WikipediaMobile\\/\\d\\.\\d(\\.\\d)?");
    private Pattern firefox = Pattern.compile(Pattern.quote("Mozilla/5.0 (Mobile; rv:18.0) Gecko/18.0 Firefox/18.0")); //VERIFIED
    private Pattern rim = Pattern.compile(Pattern.quote("Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+"));
    private Pattern windows = Pattern.compile(Pattern.quote("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)")); //VERIFIED

    private Map<String, Pattern> mobileAppPatterns;

    /**
     * * UserAgentClassifier constructor that loads:
     * 1) dClass library
     * 2) initializes Wikimedia Mobile app regular expressions
     * 3) load JSON with Apple iOS specific information
     *
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public UserAgentClassifier() throws JsonMappingException, JsonParseException {
        if (this.dw == null) {
            this.dw = new DclassWrapper();
        }
        //knownArgs.add(0,);
        this.dw.initUA();
        mobileAppPatterns = new HashMap<String, Pattern>();
        mobileAppPatterns.put("Wikimedia App Firefox", firefox);
        mobileAppPatterns.put("Wikimedia App Android", android);
        mobileAppPatterns.put("Wikimedia App RIM", rim);
        mobileAppPatterns.put("Wikimedia App Windows", windows);

        result = new Result();

        JsonToClassConverter converter = new JsonToClassConverter();
        this.appleProducts = new HashMap<String, org.wikimedia.analytics.kraken.schemas.Schema>();
        this.appleProducts = converter.construct("org.wikimedia.analytics.kraken.schemas.AppleUserAgent", "ios.json", "getProduct");
    }

    /**
     *
     * @param args
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public UserAgentClassifier(final String[] args) throws JsonMappingException, JsonParseException {
        this();
        mobileAppPatterns = new HashMap<String, Pattern>();
        appleProducts = new HashMap<String, org.wikimedia.analytics.kraken.schemas.Schema>();
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        // Check that we were passed two fields
        if (input.size() != 1) {
            throw new RuntimeException(
                    "Expected (chararray), input does not have 1 field");
        }

        try {
            // Get the types for the column and check them.  If it's
            // wrong figure out what type was passed and give a good error
            // message.
            if (input.getField(0).type != DataType.CHARARRAY) {
                String msg = "Expected input (chararray), received schema (";
                msg += DataType.findTypeName(input.getField(0).type);
                msg += ")";
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        // parentId, deviceOs and DeviceOsVersion are CHARARRAYS
        fields.add(new FieldSchema(null, DataType.CHARARRAY));
        fields.add(new FieldSchema(null, DataType.CHARARRAY));
        fields.add(new FieldSchema(null, DataType.CHARARRAY));
        //hasJavascript, isWireless and isTablet are BOOLEANS
        fields.add(new FieldSchema(null, DataType.BOOLEAN));
        fields.add(new FieldSchema(null, DataType.BOOLEAN));
        fields.add(new FieldSchema(null, DataType.BOOLEAN));
        //width and height are integers
        fields.add(new FieldSchema(null, DataType.INTEGER));
        fields.add(new FieldSchema(null, DataType.INTEGER));
        //Mobile app and addditonal vendor info are CHARARRAYS
        fields.add(new FieldSchema(null, DataType.CHARARRAY));
        fields.add(new FieldSchema(null, DataType.CHARARRAY));
        return new Schema(fields);
    }



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
    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.get(0) == null) {
            return null;
        }

        this.useragent = unspace(input.get(0).toString());
        result.classifyUseragent(this.useragent);

        Tuple output = tupleFactory.newTuple(10);
        // General properties
        output.set(0, result.getParentId());
        output.set(1, result.getDeviceOs());
        output.set(2, null); //result.getDeviceOsVersion());
        output.set(3, result.getIsWirelessDevice());
        output.set(9, result.getAjaxSupportJavascript());
        output.set(4, result.getIsTablet());
        output.set(5, result.getDisplayWidth());
        output.set(6, result.getDisplayHeight());

        //Wikimedia Mobile App detection
        output.set(7, detectMobileApp());  // field 5 contains mobile app info

        //Vendor specific handling to add more variables or clean up stuff
        if ("Apple".equals(result.getVendor())) {
            String device = result.getIsTablet() ? "iPad" : result.getModel();
            output.set(8, postProcessApple(device)); // field 6 contains additional iOS version info.
        } else if ("Samsung".equals(result.getVendor())) {
            output.set(8, postProcessSamsung((String) result.getModel()));
        } else {
            output.set(8, null);
        }

        return output;
    }

    /**
     * dClass has identified the mobile device as one from Apple but unfortunately
     * it does not provide reliable iOS version information. This function
     * adds iOS information but care should be used when this data is interpreted:
     * The iOS version is determined using the build number and hence the iOS field
     * should be read as "this mobile device has at least iOS version xyz running".
     *
     * @param device
     * @return
     * @throws ExecException
     */
    private String postProcessApple(final String device) throws ExecException {
        Matcher match = appleBuildIdentifiers.matcher(this.useragent);
        if (match.find() && device != null) {
            String build = match.group(0).toString();
            String key = device.split(" ")[0] + "-" + build;
            AppleUserAgent appleUserAgent = (AppleUserAgent) this.appleProducts.get(key);
            if (appleUserAgent != null) {
                return  appleUserAgent.toString();
            } else {
                return "unknown.apple.build.id";
            }
        } else {
            return null;
        }
    }

    /**
     * This function takes a Samsung model (GT-S5750E, GT S5620) and drops all
     * suffix characters and digits to allow for rollup of the keys.
     * @param model
     * @return
     * @throws ExecException
     */
    private String postProcessSamsung(final String model) throws ExecException {
        Matcher m = this.android.matcher(model);
        if (m.matches() && m.groupCount() == 4) {
            String name = m.group(1);
            String value = m.group(3);
            String valueCleaned = value.replaceAll("\\d", "");
            String modelCleaned = name + "-" + valueCleaned;
            return modelCleaned;
        } else {
            return m.group(0);
        }
    }

    /**
     *
     * @param useragent
     * @return useragent without spaces encoded as %20
     */
    private String unspace(String useragent) {
        return useragent.replace("%20", " ");
    }

    /**
     * If the useragent string is not identified as a mobile device using dClass
     * then we need to determine whether it's an Wikimedia mobile app. This
     * function iterates over a list of regular expressions to look for a match.
     *
     * @return String with Wikimedia Mobile app info or null if no match
     * @throws ExecException
     */
    private String detectMobileApp() throws ExecException {
        Pattern pattern;
        for (Map.Entry<String, Pattern> entry : mobileAppPatterns.entrySet()) {
            pattern = entry.getValue();
            Matcher matcher = pattern.matcher(this.useragent);
            if (matcher.find()) {
                return entry.getKey().toString();
            }
        }
        return null;
    }


    /**
     * Call the custom finalizer to free the memory from the C library
     */
    protected final void finalize() {
        try {
            super.finalize();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        this.dw.destroyUA();
    }
}
