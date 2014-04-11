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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 */
public class RegexMatch extends EvalFunc<Boolean> {
    protected Pattern pattern;

    /**
     * Constructs a UDF where regex is to be passed in later.
     */
    public RegexMatch() {
        pattern = null;
    }

    /**
     * Constructs a UDF where the regex is precompiled.
     *
     * @param regex a {@link java.lang.String} object.
     */
    public RegexMatch(String regex) {
        // use this if you want to define your regex at compile-time
        pattern = Pattern.compile(regex);
    }

    /**
     * {@inheritDoc}
     *
     * Returns a boolean on whether the given string matches the given regex.
     */
    public Boolean exec(Tuple input) throws IOException {
        String inputString = (String) input.get(0);
        // return null if input is null
        if (inputString == null) {
            return false;
        }
        // compile the given regex if it has not been defined yet
        if (pattern == null) {
            pattern = Pattern.compile((String) input.get(1));
        }
        return pattern.matcher(inputString).matches();
    }

    /** {@inheritDoc} */
    public Schema outputSchema(Schema input) {
        List<FieldSchema> arguments = new LinkedList<FieldSchema>();
        arguments.add(new FieldSchema(null, DataType.CHARARRAY));
        // require a pattern if it hasn't been defined yet
        if (pattern == null) {
            arguments.add(new FieldSchema(null, DataType.CHARARRAY));
        }
        Schema inputModel = new Schema(arguments);
        // check if input fits schema model
        if (!Schema.equals(inputModel, input, true, true)) {
            String msg = "";
            if (arguments.size() == 1) {
                msg = "\n you already defined a regex in the UDF definition, delete it if you want to use another one";
            }
            throw new IllegalArgumentException("Expected input schema "
                    + inputModel + ", received schema " + input + msg);
        }
        // output schema will be: (chararray).
        return new Schema(new FieldSchema(null, DataType.BOOLEAN));
    }
}
