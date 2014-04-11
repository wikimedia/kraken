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

    private boolean isMobileRequest;
    private boolean isSearchRequest;
    private PageviewType pageviewType;
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
        this.statusCode = statusCode.toLowerCase();
        this.ipAddress = ipAddress;
        this.mimeType = mimeType;
        this.requestMethod = requestMethod.toLowerCase();
        this.isMobileRequest = isMobileRequest();
        this.isSearchRequest = isSearchRequest();

        if (pageviewFilter == null || pageviewCanonical == null || cidrFilter == null) {
            pageviewFilter = new PageviewFilter();
            cidrFilter = new CidrFilter();
            pageviewCanonical = new PageviewCanonical(this.url);
        }
    }

    /**
     * @See https://raw.github.com/wikimedia/metrics/master/pageviews/new_mobile_pageviews_report/pageview_definition.png
     * @return boolean indicating whether this webrequest should be counted as a pageview or not.
     */
    public final boolean isWikistatsMobileReportPageview() {
        return (isValidURL()
            && pageviewFilter.isValidResponseCode(statusCode)
            && pageviewFilter.isValidRequestMethod(requestMethod)
            && pageviewFilter.isValidMimeType(url, mimeType)
            && pageviewFilter.callPageviewHandler(url, referer));
    }

    /**
     * @See https://raw.github.com/wikimedia/metrics/master/pageviews/webstatscollector/pageview_definition.png
     * XXX: For now leave out project stuff
     * @return boolean indicating whether this webrequest should be counted as a pageview or not.
     */
    public final boolean isWebstatscollectorPageview() {
        return (isValidURL()
                && this.url.getPath() != null
                && this.url.getPath().contains("/wiki/")
                && this.ipAddress != null
                && (!cidrFilter.ipAddressFallsInRange(ipAddress))
                && this.url.getHost().endsWith(".org"));
    }

    /**
     *
     * @return true/false
     */
    public final boolean isPageview() {
        return (isValidURL()
                && !getIsSearchRequest()
                && pageviewFilter.isNotBitsOrUploadDomain(url)
                && pageviewFilter.isValidMimeType(url, mimeType)
                && pageviewFilter.isValidResponseCode(statusCode)
                && pageviewFilter.isValidRequestMethod(requestMethod)
                && !cidrFilter.ipAddressFallsInRange(ipAddress)
                && pageviewFilter.isValidUserAgent(userAgent)
                && pageviewFilter.callPageviewHandler(url, referer));
    }

    /**
     * Determines whether the url was successfully parsed to instance of URL.
     * @return true/false
     */
    public final boolean isValidURL() {
        return url != null && url.getHost().contains("wiki");
    }

    /**
     *
     * @return true/false
     */
    public final boolean isSearchRequest() {
        if (url != null && url.getQuery() != null) {
            return (url.getQuery().contains("action=opensearch")
                    || url.getQuery().contains("action=search")
                    || url.getFile().contains("wiki?search")
                    || url.getQuery().contains("title=Special%3ASearch&search"));
        } else {
            return false;
        }
    }

    /**
     *
     * @return true/false
     */
    public final boolean isMobileRequest() {
        return url != null && (url.getHost().contains(".m.") || url.getHost().contains(".zero.")) ? true : false;
    }

    /**
     *
     * @return true/false
     */
    public final boolean getIsMobileRequest() {
        return isMobileRequest;
    }

    /**
     *
     * @return true/false
     */
    public final boolean getIsSearchRequest() {
        return isSearchRequest;
    }

    public final PageviewType getPageviewType() {
        return pageviewType;
    }

    public final void setPageviewType(final PageviewType pageviewType) {
        this.pageviewType = pageviewType;
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
