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
import org.apache.pig.data.Tuple;
import org.wikimedia.analytics.kraken.pageview.Pageview;
import org.wikimedia.analytics.kraken.pageview.PageviewCanonical;
import org.wikimedia.analytics.kraken.pageview.PageviewFilter;
import org.wikimedia.analytics.kraken.pageview.PageviewType;

/**
 * Entry point for the Pig UDF class that uses the Pageview filter logic.
 */
public class PageViewFilter extends FilterFunc {
    private PageviewType pageviewType;
    private PageviewFilter pageviewFilter;
    private PageviewCanonical pageviewCanonical;

    /**
     *
     * @param input tuple containing url, referer, useragent, statuscode, ip and mimetype.
     * @return true/false
     * @throws ExecException
     */
    public final Boolean exec(final Tuple input) throws ExecException {
        if (input == null || input.size() != 6) {
            return null;
        }

        String url = (String) input.get(0);
        String referer = (String) input.get(1);
        String userAgent = (input.get(2) != null ? (String) input.get(2) : "-");
        String statusCode = (input.get(3) != null ? (String) input.get(3) : "-");
        String ip = (input.get(4) != null ? (String) input.get(4) : "-");
        String mimetype = (input.get(5) != null ? (String) input.get(5) : "-");

        boolean result;

        if (url != null) {
            Pageview pageview = new Pageview(url, referer, userAgent, statusCode, ip, mimetype);
            if (pageview.validate()) {
                result = true;
            } else {
                result = false;
            }
        }  else {
            result = false;
        }
        return result;
    }
}
