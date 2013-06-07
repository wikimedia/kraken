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

import java.net.MalformedURLException;
import java.net.URL;


/**
 *
 */
public class ZeroFilterFunc extends FilterFunc {

    private String mode = null;

    /**
     *
     */
    public ZeroFilterFunc() {
        this.mode = "default";
    }

    /**
     *
     * @param modus
     */
    public ZeroFilterFunc(final String mode) {
        if (mode.equals("legacy") || mode.equals("default")) {
            this.mode = mode;
        } else {
            throw new RuntimeException(
                    "Expected mode is 'default' or 'legacy'. ");
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
           return isLegacyZeroRequest((String) input.get(0));
        }
    }

    /**
     *
     * @param input
     * @return
     */
    public final Schema outputSchema(final Schema input) {
        // Check that we were a single field
        if (input.size() != 1) {
            throw new RuntimeException(
                    "Expected (chararray), input has more than 1 fields.");
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
     * @return
     */
    private boolean isLegacyZeroRequest(final String requestedURL) {
        try {
            URL url = new URL(requestedURL);
            return url.getPath().contains("/wiki/")
                && !url.getHost().contains("meta")
                && (url.getHost().contains(".m.") || url.getHost().contains(".zero."));
        } catch (MalformedURLException e) {
            return false;
        }
    }

    /**
     *
     * @return String: the mode in which ZeroFilterFunc is running
     */
    public final String getMode() {
        return mode;
    }
}
