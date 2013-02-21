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


import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.pageview.Pageview;
import org.wikimedia.analytics.kraken.pageview.PageviewCanonical;
import org.wikimedia.analytics.kraken.pageview.PageviewFilter;
import org.wikimedia.analytics.kraken.pageview.PageviewType;

import java.net.MalformedURLException;
import java.net.URL;

public abstract class PageRequest extends EvalFunc<Tuple> {
    private URL url;
    private TupleFactory tupleFactory;
    private PageviewType pageviewType;
    private PageviewFilter pageviewFilter;
    private PageviewCanonical pageviewCanonical;

    /**
     *
     * @param input
     * @return
     * @throws ExecException
     */
    public final Tuple exec(final Tuple input) throws ExecException {
        if (input == null || input.size() != 5) {
            return null;
        }
        String url = (String) input.get(0);
        String referer = (String) input.get(1);
        String userAgent = (String) input.get(2);
        String statusCode = (String) input.get(3);
        String ip = (String) input.get(4);
        String mimetype = (String) input.get(5);

        setUrl(url);

        Tuple output = tupleFactory.newTuple(1);
        if (this.url != null
            && pageviewFilter.isValidResponseCode(statusCode)
            && pageviewFilter.isValidMimeType(mimetype)
            && pageviewFilter.isNotInternalWMFTraffic(ip)) {

            Pageview pageview = new Pageview(url, referer, userAgent, statusCode, ip, mimetype);
            if (pageview.validate()) {
                output.set(0, true);
            } else {
                output.set(0, false);
            }
        }  else {
            output.set(0, false);
        }
        return output;
    }


    /**
     * Setter for the url field.
     * @param urlStr
     */
    private void setUrl(final String urlStr) {
        try {
            this.url = new URL(urlStr);
        } catch (MalformedURLException e) {
            warn("Supplied url string is not a valid URI.", PigWarning.UDF_WARNING_1);
            this.url = null;
        }
    }
}