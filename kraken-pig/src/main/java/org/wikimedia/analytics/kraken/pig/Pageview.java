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
import org.wikimedia.analytics.kraken.pageview.PageviewCanonical;
import org.wikimedia.analytics.kraken.pageview.PageviewFilter;
import org.wikimedia.analytics.kraken.pageview.PageviewType;

import java.net.MalformedURLException;
import java.net.URL;

public abstract class Pageview extends EvalFunc<Tuple> {
    private URL url;
    private TupleFactory tupleFactory;
    private PageviewType pageviewType;
    private PageviewFilter pageviewFilter;
    private PageviewCanonical pageviewCanonical;



    public Pageview() {
        tupleFactory = TupleFactory.getInstance();
        pageviewFilter = new PageviewFilter();
        pageviewCanonical = new PageviewCanonical();

    }

    private boolean passCustomFilter() {
        switch (this.pageviewType) {
            case MOBILE:

                break;

            case DESKTOP:

                break;

            case API:

                break;

            case BLOG:
                return this.pageviewFilter.isValidBlogPageview(this.url);

            default:
                return false;
        }
        return false;
    }

    /**
     *
     * @return
     */
    private String canonicalizeURL()  {
        switch (this.pageviewType) {
            case MOBILE:
                return pageviewCanonical.canonicalizeDesktopPageview(this.url, this.pageviewType);

            case DESKTOP:
                return pageviewCanonical.canonicalizeMobilePageview(this.url, this.pageviewType);

            case API:
                return pageviewCanonical.canonicalizeApiRequest(this.url, this.pageviewType);

            case BLOG:
                return pageviewCanonical.canonicalizeBlogPageview(this.url, this.pageviewType);

            default:
                return this.url.toString();
        }
    }

    /**
     * Given a url, determine the pageview type (mobile, desktop, api, and blog).
     */
    private void detectPageviewType() {
        if (this.url.getHost().contains(".m.")) {
            this.pageviewType = PageviewType.MOBILE;
        } else if (this.url.getPath().contains("/wiki/")) {
            this.pageviewType = PageviewType.DESKTOP;
        } else if (this.url.getPath().contains("api.php")) {
            this.pageviewType = PageviewType.API;
        } else if (this.url.getHost().contains("blog")) {
            this.pageviewType = PageviewType.BLOG;
        }


    }

    public Tuple exec(final Tuple input) throws ExecException {
        if (input == null || input.size() != 4) {
            return null;
        }
        String url = (String) input.get(0);
        String response = (String) input.get(1);
        String mimetype = (String) input.get(2);
        String ip = (String) input.get(3);

        setUrl(url);


        if (pageviewFilter.isValidResponseCode(response)
            && pageviewFilter.isValidMimeType(mimetype)
            && !pageviewFilter.isInternalWMFTraffic(ip)) {

            detectPageviewType();
            if (passCustomFilter()) {
                String canonicalURL = canonicalizeURL();
                Tuple output = tupleFactory.newTuple(1);
                output.set(0, canonicalURL);
                return output;
            }
            return null;
        }
        return null;
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
