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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.wikimedia.analytics.kraken.pageview.UserAgent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    public UserAgentClassifier() {}

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        // Check that we were passed two fields
        if (input.size() != 1) {
            throw new RuntimeException("Expected (chararray), input does not have 1 field");
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
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Device Vendor
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Device Model
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Device OS
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Device OS Version
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Device Class
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Browser
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Browser Version
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // WMF Mobile App ID
        fields.add(new FieldSchema(null, DataType.BOOLEAN));        // hasJavaScript
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // displayDimensions: "WIDTH x HEIGHT"
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Input Devices
        fields.add(new FieldSchema(null, DataType.CHARARRAY));      // Non WMF Mobile App ID
        return new Schema(fields);
    }


    /**
     * {@inheritDoc}
     *
     * Method exec takes a {@link Tuple} which should contain a single field, namely the user agent string.
     *
     * returns a tuple with the following fields:
     */
    @Override
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.get(0) == null) {
            return null;
        }

        UserAgent userAgent = new UserAgent(input.get(0).toString());

        Tuple output = tupleFactory.newTuple(12);
        output.set(0, userAgent.getParentId()); // XXX: should be vendor?
        output.set(1, userAgent.getModel());
        output.set(2, userAgent.getDeviceOs());
        output.set(3, userAgent.getDeviceOsVersion());
        output.set(4, userAgent.getDeviceClass());
        output.set(5, userAgent.getBrowser());
        output.set(6, userAgent.getBrowserVersion());
        output.set(7, userAgent.getWMFMobileApp());
        output.set(8, userAgent.getAjaxSupportJavascript());
        output.set(9, userAgent.getDisplayDimensions());
        output.set(10, userAgent.getInputDevices());
        output.set(11, userAgent.getNonWMFMobileApp());

        return output;
    }

}
