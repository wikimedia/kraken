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
 *
 */
public class Pageview {
    private URL url;
    private URL referer;
    private String userAgent;
    private String statusCode;
    private String ipAddress;
    private String mimeType;
    private String timestamp;

    private PageviewType pageviewType;
    private PageviewFilter pageviewFilter;
    private PageviewCanonical pageviewCanonical;

    /**
     *
     * @param url
     * @param referer
     * @param userAgent
     * @param statusCode
     * @param ipAddress
     * @param mimeType
     */
    public Pageview(final String url, final String referer, final String userAgent,
                    final String statusCode, final String ipAddress, final String mimeType) {

        try {
            this.url = new URL(url);
            this.referer = new URL(referer);
            detectPageviewType();
        } catch (MalformedURLException e) {
            this.url = null;
            this.referer = null;
        }

        this.userAgent = userAgent;
        this.statusCode = statusCode;
        this.ipAddress = ipAddress;
        this.mimeType = mimeType;

        if (pageviewFilter == null || pageviewCanonical == null) {
            pageviewFilter = new PageviewFilter();
            pageviewCanonical = new PageviewCanonical();
        }
    }

    /**
     *
     * @return
     */
    private boolean passCustomFilter() {
        switch (this.pageviewType) {
            case MOBILE :
                return pageviewFilter.isValidMobilePageview(this.url);

            case MOBILE_API:
                return pageviewFilter.isValidMobileAPIPageview(this.url, this.referer);

            case DESKTOP:
                return pageviewFilter.isValidDesktopPageview(this.url);

            case API:
                return true;

            case BLOG:
                return pageviewFilter.isValidBlogPageview(this.url);

            case SEARCH:
                // Discard all search queries by default
                return false;

            default:
                return false;
        }
    }

    /**
     *
     * @return
     */
    private String canonicalizeURL()  {
        System.out.println(this.pageviewType.toString());
        switch (this.pageviewType) {
            case MOBILE:
                return pageviewCanonical.canonicalizeMobilePageview(this.url,  this.pageviewType);

            case MOBILE_API:
                return pageviewCanonical.canonicalizeMobilePageview(this.url,  this.pageviewType);

            case DESKTOP:
                return pageviewCanonical.canonicalizeDesktopPageview(this.url,  this.pageviewType);

            case API:
                return pageviewCanonical.canonicalizeApiRequest(this.url,  this.pageviewType);

            case BLOG:
                return pageviewCanonical.canonicalizeBlogPageview(this.url,  this.pageviewType);

            case SEARCH:
                return pageviewCanonical.canonicalizeSearchQuery(this.url,  this.pageviewType);

            case IMAGE:
                return pageviewCanonical.canonicalizeImagePageview(this.url,  this.pageviewType);

            default:
                return (this.url != null ? this.url.toString() : "bad.formed.url");
        }
    }

    /**
     *
     * @return
     */
    public final String getCanonicalURL() {
        return canonicalizeURL();
    }

    /**
     * Given a url, determine the pageview type (mobile, desktop, api, search and blog).
     */
    public void detectPageviewType() {
         if (this.url.getHost().contains(".m.")) {
            if (this.url.getPath().contains("api.php")) {
                this.pageviewType = PageviewType.MOBILE_API;
            } else {
                this.pageviewType = PageviewType.MOBILE;
            }
        }

        else if (this.url.getPath().contains("/wiki/")) {
            this.pageviewType = PageviewType.DESKTOP;
        } else if (this.url.getPath().contains("index.php")) {
            this.pageviewType = PageviewType.DESKTOP;
        } else if (this.url.getPath().contains("api.php")) {
             if (this.url.getPath().contains("action=opensearch")) {
                 this.pageviewType = PageviewType.SEARCH;
             } else {
                this.pageviewType = PageviewType.API;
             }
        } else if (this.url.getHost().contains("commons") || this.url.getHost().contains("upload")) {
            this.pageviewType = PageviewType.IMAGE;
        } else if (this.url.getHost().contains("blog")) {
            this.pageviewType = PageviewType.BLOG;
        } else {
            this.pageviewType = PageviewType.DESKTOP;
        }
    }

    /**
     *
     * @return
     */
    public final boolean validate() {
        boolean result = false;
        if (pageviewFilter.isValidUserAgent(userAgent)
                && pageviewFilter.isValidResponseCode(statusCode)
                && pageviewFilter.isValidMimeType(mimeType)
                && !pageviewFilter.isInternalWMFTraffic(ipAddress)) {
            result = passCustomFilter();
            return result;
        }
        return result;
    }

    /**
     *
     * @return
     */
    public final boolean isValidURL() {
        if (this.url != null) {
            return true;
        } else {
            return false;
        }
    }
}

