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
    //private String mode;

    private PageviewType pageviewType;
    private PageviewType pageviewTypeReferer;
    private PageviewFilter pageviewFilter;
    private PageviewCanonical pageviewCanonical;
    private ProjectInfo projectInfo;
    private CidrFilter cidrFilter;

    /**
     * All passed in strings will be converted to lowercase and stored to this instance
     * @param url page visited
     * @param referer origin of visitor, '-' if direct hit.
     * @param userAgent string indicating the browser/device used by the visitor
     * @param statusCode responsecode from the cache server to indicate whether request was successful or not
     * @param ipAddress ipaddress of the visitor
     * @param mimeType content type requested
     * @param requestMethod GET, POST, etc.
     */
    public Pageview(String url, String referer, String userAgent,
                    String statusCode, String ipAddress, String mimeType, String requestMethod) {

        // null coalesce all the fields to empty string
        url = url == null ? "" : url;
        referer = referer == null ? "" : referer;
        userAgent = userAgent == null ? "" : userAgent;
        statusCode = statusCode == null ? "" : statusCode;
        ipAddress = ipAddress == null ? "0.0.0.0" : ipAddress;
        mimeType = mimeType == null ? "" : mimeType;
        requestMethod = requestMethod == null ? "" : requestMethod;

        try {
            this.url = new URL(url.toLowerCase());
        } catch (MalformedURLException e) {
            this.url = null;
        }

        try {
            this.referer = new URL(referer.toLowerCase());
        }  catch (MalformedURLException e) {
            this.referer = null;
        }

        this.userAgent = userAgent;
        this.statusCode = statusCode.toLowerCase();
        this.ipAddress = ipAddress;
        this.mimeType = mimeType.toLowerCase();
        this.requestMethod = requestMethod.toLowerCase();
        //this.mode = "new_definition";

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
        switch (pageviewType) {
            case MOBILE:
                return pageviewFilter.isValidMobilePageview(url);

            case MOBILE_ZERO:
                return pageviewFilter.isValidMobilePageview(url);

            case MOBILE_API:
                return pageviewFilter.isValidMobileAPIPageview(url, referer);

            case MOBILE_SEARCH:
                // Discard all search queries by default
                return false;

            case DESKTOP:
                return pageviewFilter.isValidDesktopPageview(url);

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
                return pageviewFilter.isValidBlogPageview(url);

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
        switch (pageviewType) {
            case MOBILE:
                pageviewCanonical.canonicalizeMobilePageview(url, pageviewType);

            case MOBILE_API:
                pageviewCanonical.canonicalizeMobilePageview(url, pageviewType);

            case MOBILE_ZERO:
                pageviewCanonical.canonicalizeMobilePageview(url, pageviewType);

            case MOBILE_SEARCH:
                pageviewCanonical.canonicalizeSearchQuery(url, pageviewType);

            case DESKTOP:
                pageviewCanonical.canonicalizeDesktopPageview(url, pageviewType);

            case DESKTOP_API:
                pageviewCanonical.canonicalizeApiRequest(url, pageviewType);

            case DESKTOP_SEARCH:
                pageviewCanonical.canonicalizeSearchQuery(url, pageviewType);

            case COMMONS_IMAGE:
                pageviewCanonical.canonicalizeImagePageview(url, pageviewType);

            case BANNER:
                //TODO: not yet implemented
                break;

            case BLOG:
                pageviewCanonical.canonicalizeBlogPageview(url, pageviewType);
                break;

            default:
                break;
        }
    }




    /**
     * @See https://raw.github.com/wikimedia/metrics/master/pageviews/new_mobile_pageviews_report/pageview_definition.png
     * @return boolean indicating whether this webrequest should be counted as a pageview or not.
     */
    public final boolean isWikistatsMobileReportPageview() {
        pageviewType = PageviewType.determinePageviewType(url);
        pageviewTypeReferer =  PageviewType.determinePageviewType(referer);
        return (pageviewFilter.isValidResponseCode(statusCode)
                && pageviewFilter.isValidRequestMethod(requestMethod)
                && pageviewFilter.isValidMimeType(pageviewType, mimeType)
                && !pageviewFilter.refersToSameArticle(pageviewType, url, pageviewTypeReferer, referer));
    }
    /**
     * @See https://raw.github.com/wikimedia/metrics/master/pageviews/webstatscollector/pageview_definition.png
     * XXX: For now leave out project stuff
     * @return boolean indicating whether this webrequest should be counted as a pageview or not.
     */
    public final boolean isWebstatscollectorPageview() {
        return (isValidURL()
                && !cidrFilter.ipAddressFallsInRange(this.ipAddress))
                && this.url.getHost().endsWith(".org")
                && this.url.getPath() != null
                && this.url.getPath().contains("/wiki/");
    }

    public final boolean isPageview() {
        if (initialPageviewValidation()) {
            pageviewType = PageviewType.determinePageviewType(url);
            return secondStepPageviewValidation();
        } else {
            return false;
        }
    }

    /**
     * Pageviewtype agnostic checks to determine whether request is pageview or not.
     * @return true/false
     */
    public final boolean initialPageviewValidation() {
        return (isValidURL()
                && pageviewFilter.isNotBitsOrUploadDomain(url)
                && pageviewFilter.isValidUserAgent(userAgent)
                && pageviewFilter.isValidResponseCode(statusCode)
                && pageviewFilter.isValidRequestMethod(requestMethod)
                && !cidrFilter.ipAddressFallsInRange(ipAddress));
    }

    /**
     * Determines whether the url was successfully parsed to instance of URL.
     * @return true/false
     */
    public final boolean isValidURL() {
        return url != null;
    }

    public final PageviewType getPageviewType() {
        return pageviewType;
    }

    public final PageviewFilter getPageviewFilter() {
        return pageviewFilter;
    }

    public final PageviewCanonical getPageviewCanonical() {
        return pageviewCanonical;
    }

    public final ProjectInfo getProjectInfo() {
        if (projectInfo == null && url != null) {
            projectInfo = new ProjectInfo(url.getHost());
        }
        return projectInfo;
    }

    public final CidrFilter getCidrFilter() {
        return cidrFilter;
    }

}
