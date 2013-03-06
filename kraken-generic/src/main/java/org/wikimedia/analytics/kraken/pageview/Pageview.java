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
 *
 *
 1. Hostname of the squid
 2. Sequence number
 3. The current time in ISO 8601 format (plus milliseconds), according to the squid server's clock.
 4. Request service time in ms
 5. Client IP
 6. Squid request status, HTTP status code
 7. Reply size including HTTP headers
 8. Request method (GET/POST etc)
 9. URL
 10. Squid hierarchy status, peer IP
 11. MIME content type
 12. Referer header
 13. X-Forwarded-For header
 14. User-Agent header
 15. Accept_Language
 16 X-CS (Wikipedia Zero MCC-MNC Carrier Code)
 */
public class Pageview {
    private URL url;
    private URL referer;
    private String userAgent;
    private String statusCode;
    private String ipAddress;
    private String mimeType;
    private String requestMethod;

    private PageviewType pageviewType;
    private PageviewFilter pageviewFilter;
    private PageviewCanonical pageviewCanonical;
    private CidrFilter cidrFilter;

    /**
     *
     * @param url page visited
     * @param referer origin of visitor, '-' if direct hit.
     * @param userAgent string indicating the browser/device used by the visitor
     * @param statusCode responsecode from the cache server to indicate whether request was successful or not
     * @param ipAddress ipaddress of the visitor
     * @param mimeType content type requested
     * @param requestMethod
     */
    public Pageview(final String url, final String referer, final String userAgent,
                    final String statusCode, final String ipAddress, final String mimeType, final String requestMethod) {

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
        this.requestMethod = requestMethod;

        if (pageviewFilter == null || pageviewCanonical == null || cidrFilter == null) {
            pageviewFilter = new PageviewFilter();
            cidrFilter = new CidrFilter();
            pageviewCanonical = new PageviewCanonical();
        }
    }

    /**
     * Detailed business logic to determine per pageview type whether the visit should be counted as a pageview or not.
     * @return true/false
     */
    public final boolean secondStepPageviewValidation() {
        switch (this.pageviewType) {
            case MOBILE:
                return pageviewFilter.isValidMobilePageview(this.url);

            case MOBILE_ZERO:
                return pageviewFilter.isValidMobilePageview(this.url);

            case MOBILE_API:
                return pageviewFilter.isValidMobileAPIPageview(this.url, this.referer);

            case MOBILE_SEARCH:
                // Discard all search queries by default
                return false;

            case DESKTOP:
                return pageviewFilter.isValidDesktopPageview(this.url);

            case DESKTOP_API:
                return true;

            case DESKTOP_SEARCH:
                // Discard all search queries by default
                return false;

            case COMMONS_IMAGE:
                return true;

            case BANNER:
                // Discard all banner impressions by default
                return false;

            case BLOG:
                return pageviewFilter.isValidBlogPageview(this.url);

            case OTHER:
                // Request that not match to any of the categories above should not be filtered but should show up
                // so we can refine this categorization even further.
                return true;

            default:
                // Request that not match to any of the categories above should not be filtered but should show up
                // so we can refine this categorization even further.
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

            case MOBILE_ZERO:
                pageviewCanonical.canonicalizeMobilePageview(this.url, this.pageviewType);

            case MOBILE_SEARCH:
                pageviewCanonical.canonicalizeSearchQuery(this.url, this.pageviewType);

            case DESKTOP:
                pageviewCanonical.canonicalizeDesktopPageview(this.url, this.pageviewType);

            case DESKTOP_API:
                pageviewCanonical.canonicalizeApiRequest(this.url, this.pageviewType);

            case DESKTOP_SEARCH:
                pageviewCanonical.canonicalizeSearchQuery(this.url, this.pageviewType);

            case COMMONS_IMAGE:
                pageviewCanonical.canonicalizeImagePageview(this.url, this.pageviewType);

            case BANNER:
                //TODO: not yet implemented
                break;

            case BLOG:
                pageviewCanonical.canonicalizeBlogPageview(this.url, this.pageviewType);

            default:
                break;
        }
    }

    /**
     * Given a url, determine the pageview type (mobile, desktop, api, search and blog).
     */
    public final void determinePageviewType() {
        if (this.url.getQuery() != null && this.url.getQuery().contains("BannerLoader")) {
            this.pageviewType = PageviewType.BANNER;
        } else if (this.url.getHost().contains("commons")) {
            this.pageviewType = PageviewType.COMMONS_IMAGE;
        } else if (this.url.getHost().contains(".m.")) {
            this.pageviewType = PageviewType.MOBILE;
            determineMobileSubPageviewType();
        } else if (this.url.getHost().contains(".zero.")) {
            this.pageviewType = PageviewType.MOBILE_ZERO;
        } else if (this.url.getHost().contains("wiki")) {
            this.pageviewType = PageviewType.DESKTOP;
            determineDesktopSubPageviewType();
        } else if (this.url.getHost().contains("blog")) {
            this.pageviewType = PageviewType.BLOG;
        } else {
            this.pageviewType = PageviewType.OTHER;
        }
    }

    /**
     *
     */
    private void determineDesktopSubPageviewType() {
        if (this.url.getPath().contains("api.php")) {
            if (this.url.getQuery() != null && this.url.getQuery().contains("opensearch")) {
                this.pageviewType = PageviewType.DESKTOP_SEARCH;
            } else {
                this.pageviewType = PageviewType.DESKTOP_API;
            }
        } else if (this.url.getQuery() != null && this.url.getQuery().contains("search")) {
            this.pageviewType = PageviewType.DESKTOP_SEARCH;
        }
    }

    /**
     *
     */
    private void determineMobileSubPageviewType() {
        if (this.url.getPath().contains("api.php")) {
            if (this.url.getQuery() != null && this.url.getQuery().contains("opensearch")) {
                this.pageviewType = PageviewType.MOBILE_SEARCH;
            }  else {
                this.pageviewType = PageviewType.MOBILE_API;
            }
        } else if (this.url.getQuery() != null && this.url.getQuery().contains("search")) {
            this.pageviewType = PageviewType.MOBILE_SEARCH;
        }
    }

    /**
     *
     */
    public final boolean isPageview() {
        if (initialPageviewValidation()) {
            determinePageviewType();
            if (secondStepPageviewValidation()) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Pageviewtype agnostic checks to determine whether request is pageview or not.
     * @return true/false
     */
    public final boolean initialPageviewValidation() {
        if (isValidURL()
                && pageviewFilter.isNotBitsOrUploadDomain(this.url)
                && pageviewFilter.isValidUserAgent(this.userAgent)
                && pageviewFilter.isValidResponseCode(this.statusCode)
                && pageviewFilter.isValidRequestMethod(this.requestMethod)
                && cidrFilter.ipAddressFallsInRange(this.ipAddress)) {
            return true;
        }  else {
            return false;
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

    /**
     *
     * @return
     */
    public final PageviewType getPageviewType() {
        return pageviewType;
    }

    /**
     *
     * @return
     */
    public final PageviewFilter getPageviewFilter() {
        return pageviewFilter;
    }

    /**
     *
     * @return
     */
    public final PageviewCanonical getPageviewCanonical() {
        return pageviewCanonical;
    }

    /**
     *
     * @return
     */
    public final CidrFilter getCidrFilter() {
        return cidrFilter;
    }

}

