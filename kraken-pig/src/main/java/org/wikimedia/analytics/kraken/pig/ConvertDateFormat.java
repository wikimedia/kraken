/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Modified for Kraken to accept arguments at run-time.

 */

package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class ConvertDateFormat extends EvalFunc<String> {

    public static enum ERRORS { DateParseError };

    private SimpleDateFormat inputSdf;
    private SimpleDateFormat outputSdf;

    /**
     * <p>Constructor for ConvertDateFormat.</p>
     */
    public ConvertDateFormat() {
        inputSdf = null;
        outputSdf = null;
    }

    /**
     * <p>Constructor for ConvertDateFormat.</p>
     *
     * @param inputDateFormat a {@link java.lang.String} object.
     * @param outputDateFormat a {@link java.lang.String} object.
     */
    public ConvertDateFormat(final String inputDateFormat, final String outputDateFormat) {
        this.inputSdf = new SimpleDateFormat(inputDateFormat);
        this.outputSdf = new SimpleDateFormat(outputDateFormat);
    }

    /** {@inheritDoc} */
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        //Take second argument if inputSdf or outputSdf are not initialized.
        if (inputSdf == null || outputSdf == null) {
            this.inputSdf = new SimpleDateFormat((String) input.get(1));
            this.outputSdf = new SimpleDateFormat((String) input.get(2));
        }

        String s = null;
        try {
            Date d = inputSdf.parse((String) input.get(0));
            s = outputSdf.format(d);
        } catch (ParseException e) {
            pigLogger.warn(this, "Date parsing error", ERRORS.DateParseError);
        }

        return s;
    }

}
