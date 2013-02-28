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
package org.wikimedia.analytics.kraken.pageview;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * This class provides the main functionality to:
 * 1) determine whether a logline from a cache server is a pageview
 * 2) canonicalize the title of the pageview
 */
public class Pageview {
    private URL url;
    private URL referer;
    private String userAgent;
    private String statusCode;
    private String ipAddress;
    private String mimeType;
    private String timestamp;

    public PageviewType pageviewType;
    public PageviewFilter pageviewFilter;
    public PageviewCanonical pageviewCanonical;
    public CidrFilter cidrFilter;

    /**
     *
     * @param url page visited
     * @param referer origin of visitor, '-' if direct hit.
     * @param userAgent string indicating the browser/device used by the visitor
     * @param statusCode responsecode from the cache server to indicate whether request was successful or not
     * @param ipAddress ipaddress of the visitor
     * @param mimeType content type requested
     */
    public Pageview(final String url, final String referer, final String userAgent,
                    final String statusCode, final String ipAddress, final String mimeType) {

        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            this.url = null;
        }

        try {
            this.referer = new URL(referer);
        }  catch (MalformedURLException e) {
            this.referer = null;
        }

        this.userAgent = userAgent;
        this.statusCode = statusCode;
        this.ipAddress = ipAddress;
        this.mimeType = mimeType;

        detectPageviewType();

        if (pageviewFilter == null || pageviewCanonical == null) {
            pageviewFilter = new PageviewFilter();
            cidrFilter = new CidrFilter();
            pageviewCanonical = new PageviewCanonical();
        }
    }

    /**
     * Detailed business logic to determine per pageview type whether the visit should be counted as a pageview or not.
     * @return true/false
     */
    public final boolean passCustomFilter() {
        if (this.pageviewType == null) {
            return true;
        }
        switch (this.pageviewType) {
            case MOBILE:
                return pageviewFilter.isValidMobilePageview(this.url);

            case MOBILE_API:
                return pageviewFilter.isValidMobileAPIPageview(this.url, this.referer);

            case DESKTOP:
                return pageviewFilter.isValidDesktopPageview(this.url);

            case IMAGE:
                return true;

            case API:
                return true;

            case BLOG:
                return pageviewFilter.isValidBlogPageview(this.url);

            case SEARCH:
                // Discard all search queries by default
                return false;

            case OTHER:
                // Discard all other requests
                return false;

            default:
                return true;
        }
    }

    /**
     *
     * @return String containing the canonical title of the page visited
     */
    public final void canonicalizeURL()  {
        switch (this.pageviewType) {
            case MOBILE:
                pageviewCanonical.canonicalizeMobilePageview(this.url, this.pageviewType);

            case MOBILE_API:
                pageviewCanonical.canonicalizeMobilePageview(this.url, this.pageviewType);

            case DESKTOP:
                pageviewCanonical.canonicalizeDesktopPageview(this.url, this.pageviewType);

            case API:
                pageviewCanonical.canonicalizeApiRequest(this.url, this.pageviewType);

            case BLOG:
                pageviewCanonical.canonicalizeBlogPageview(this.url, this.pageviewType);

            case SEARCH:
                pageviewCanonical.canonicalizeSearchQuery(this.url, this.pageviewType);

            case IMAGE:
                pageviewCanonical.canonicalizeImagePageview(this.url, this.pageviewType);

            default:
                break;
        }
    }

    /**
     * Given a url, determine the pageview type (mobile, desktop, api, search and blog).
     */
    public final void detectPageviewType() {
        if (this.url == null) {
            this.pageviewType = PageviewType.DESKTOP;
        } else if (this.url.getHost().contains(".m.")) {
            if (this.url.getPath().contains("api.php")) {
                if (this.url.getQuery() != null && this.url.getQuery().contains("opensearch")) {
                    this.pageviewType = PageviewType.MOBILE_SEARCH;
                } else if (this.mimeType.startsWith("image")) {
                    this.pageviewType = PageviewType.OTHER;
                }  else {
                    this.pageviewType = PageviewType.MOBILE_API;
                }
            } else {
                this.pageviewType = PageviewType.MOBILE;
            }
        } else if (this.url.getPath().contains("/wiki/")) {
            this.pageviewType = PageviewType.DESKTOP;
        } else if (this.url.getPath().contains("index.php")) {
            this.pageviewType = PageviewType.DESKTOP;
        } else if (this.url.getPath().contains("api.php")) {
            if (this.url.getQuery() != null && this.url.getQuery().contains("opensearch")) {
                this.pageviewType = PageviewType.SEARCH;
            } else {
                this.pageviewType = PageviewType.API;
            }
        } else if (this.url.getHost().contains("commons") || this.url.getHost().contains("upload")) {
            this.pageviewType = PageviewType.IMAGE;
        } else if (this.url.getHost().contains("blog")) {
            this.pageviewType = PageviewType.BLOG;
        } else {
            this.pageviewType = PageviewType.OTHER;
        }
    }

    /**
     * Pageviewtype agnostic checks to determine whether request is pageview or not.
     * @return true/false
     */
    public final boolean validate() {
        boolean result = false;
        if (isValidURL()
                && pageviewFilter.isValidUserAgent(this.userAgent)
                && pageviewFilter.isValidResponseCode(this.statusCode)
                && pageviewFilter.isValidMimeType(this.mimeType)
                && cidrFilter.ipAddressFallsInRange(this.ipAddress)) {
            result = passCustomFilter();
            return result;
        }  else {
            return result;
        }
    }

    /**
     * Determines whether the url was successfully parsed to instance of URL.
     * @return true/false
     */
    public final boolean isValidURL() {
        if (this.url != null) {
            return true;
        } else {
            return false;
        }
    }
}

