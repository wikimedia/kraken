/**
 * Copyright (C) 2012-2013  Wikimedia Foundation

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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.wikimedia.analytics.kraken.pageview.UserAgent;

import java.io.IOException;

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

        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("vendor", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("model", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("device_os", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("device_os_version", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("device_class", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("browser", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("browser_version", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("wmf_mobile_app", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("has_javascript", DataType.BOOLEAN));
        tupleSchema.add(new Schema.FieldSchema("display_dimensions", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("input_device", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("non_wmf_mobile_app", DataType.CHARARRAY));
        Schema ret;
        try {
            ret = new Schema(new Schema.FieldSchema("dclass", tupleSchema, DataType.TUPLE));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
        return ret;
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
