/**
 * Copyright (C) 2012  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.pageview;


import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.*;

public class PageviewTest {

    Pageview pageview;

    @Test
    public void testMobilePageview() {
        String logLines = "ssl1002,362176022,1970-01-01T00:00:00.000,0.086,0.0.0.0,FAKE_CACHE_STATUS/301,680,GET https://fr.wikipedia.org/wiki/Discussion:Histoire du Racing Club de Strasbourg,NONE/wikipedia,-,-,-,Mozilla/5.0%20(compatible;%20Googlebot/2.1;%20+http://www.google.com/bot.html),-,-";
        String[] logFields = logLines.split(",");
        pageview = new Pageview(logFields[8], logFields[12], logFields[14], logFields[5], logFields[6], logFields[11], logFields[7]);
        assertFalse(pageview.isPageview());
    }

    @Test
    public void testMobileApiRequest() throws MalformedURLException{
        String url = "http://de.m.wikipedia.org/w/api.php?action=mobileview&page=mobiletoken&override=1&format=xml";
        String referer = "-";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text"; //This is why it causes the test to fail for isPageview and isWikistatsMobileReportPageview
        String requestMethod = "get";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        pageview.setPageviewType(PageviewType.determinePageviewType(new URL(url)));
        assertEquals(PageviewType.API, pageview.getPageviewType());
        assertFalse(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testDropMobileSearchRequest1() throws MalformedURLException {
        String url = "http://fr.m.wikipedia.org/w/api.php?action=opensearch&limit=15&namespace=0&format=xml&search=kacey%20jor";
        String referer = "-";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text";
        String requestMethod = "get";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        pageview.setPageviewType(PageviewType.determinePageviewType(new URL(url)));
        assertEquals(PageviewType.API, pageview.getPageviewType());
        assertTrue(pageview.isSearchRequest());
        assertFalse(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    /**
     *  test r4 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testDropMobileSearchRequest2() throws MalformedURLException {
        String url = "http://en.m.wikipedia.org/wiki?search=Waylon%20Smithers";
        String referer = "-";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "get";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        pageview.setPageviewType(PageviewType.determinePageviewType(new URL(url)));
        assertEquals(PageviewType.OTHER, pageview.getPageviewType());
        assertTrue(pageview.isMobileRequest());
        assertTrue(pageview.isSearchRequest());
        assertFalse(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }


    @Test
    public void testZeroRequest() throws MalformedURLException {
        String url = "http://th.zero.wikipedia.org/wiki/";
        String referer = "-";
        String userAgent = "useragent";
        String statusCode = "302";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        pageview.setPageviewType(PageviewType.determinePageviewType(new URL(url)));
        assertEquals(PageviewType.REGULAR, pageview.getPageviewType());
        assertTrue(pageview.isMobileRequest());
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testMobileSpecialPage() {
        String url = "http://en.m.wikipedia.org/wiki/Special:MobileMenu";
        String referer = "http://fr.m.wikipedia.org/";
        String userAgent = "useragent";
        String statusCode = "304";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html; charset=UTF-8";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }


    @Test
    public void testZeroBannerRequest() throws MalformedURLException {
        String url = "http://fr.m.wikipedia.org/wiki/Folklore?zeropartner=1006&renderZeroRatedBanner=true";
        String referer = "http://fr.m.wikipedia.org/";
        String userAgent = "useragent";
        String statusCode = "304";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html; charset=UTF-8";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertEquals(PageviewType.REGULAR, PageviewType.determinePageviewType(new URL(url)));
        assertTrue(pageview.isMobileRequest());
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testZeroBannerRequest2() {
        String url = "http://fr.m.wikipedia.org/wiki/Folklore?zeropartner=1006&renderZeroRatedBanner=true";
        String referer = "http://fr.m.wikipedia.org/";
        String userAgent = "useragent";
        String statusCode = "miss/200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/vnd.wap.wml";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testBannerLoader() {
        String url = "http://en.wikipedia.org/wiki/notSureAboutThis?BannerLoader=1";
        String referer = "http://www.google.com";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertFalse(pageview.isPageview()); // TODO: when banner impressions are pageviews, turn this back on
        assertTrue(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testMobilePageview2() throws MalformedURLException {
        String url = "http://ar.m.wikipedia.org/wiki/%D9%85%D9%84%D9%81:Saudi_Ranks.JPG";
        String referer = "http://fr.m.wikipedia.org/";
        String userAgent = "useragent";
        String statusCode = "miss/200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html; charset=UTF-8";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertEquals(PageviewType.REGULAR, PageviewType.determinePageviewType(new URL(url)));
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());

    }

    @Test
    public void testNullURL() {
        String url = null;
        String referer = "http://en.wikipedia.org/wiki/Sinkhole";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertFalse(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testNullReferer() {
        String url = "http://en.wikipedia.org/wiki/Sinkhole";
        String referer = null;
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testNullUseragent() {
        String url = "http://en.wikipedia.org/wiki/Sinkhole";
        String referer = "http://www.google.com";
        String userAgent = null;
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testNullStatusCode() {
        String url = "http://en.wikipedia.org/wiki/Sinkhole";
        String referer = "http://www.google.com";
        String userAgent = "useragent";
        String statusCode = null;
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertFalse(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testNullIPAddress() {
        String url = "http://en.wikipedia.org/wiki/Sinkhole";
        String referer = "http://www.google.com";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = null;
        String mimeType = "text/html";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertTrue(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testNullMimeType() {
        String url = "http://en.wikipedia.org/wiki/Sinkhole";
        String referer = "http://www.google.com";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = null;
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertFalse(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testNullRequestMethod() {
        String url = "http://en.wikipedia.org/wiki/Sinkhole";
        String referer = "http://www.google.com";
        String userAgent = "useragent";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "text/html";
        String requestMethod = null;

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertFalse(pageview.isPageview());
        assertTrue(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }


    @Test
    public void testSampleMobileLogLine() {
        String url = "http://fr.m.wikipedia.org/w/api.php?action=opensearch&search=balbutier+&format=json";
        String referer = "-";
        String userAgent = "WikipediaMobile/ something Android something";
        String statusCode = "200";
        String ipAddress = "0.0.0.0";
        String mimeType = "application/json; charset=utf-8";
        String requestMethod = "GET";

        pageview = new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertFalse(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertFalse(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testRefererPageview1() throws MalformedURLException {
        String url = "https://en.m.wikipedia.org/w/api.php?format=json&action=mobileview&page=Tornado&variant=en&redirects=yes&prop=sections%7Ctext&noheadings=yes&sectionprop=level%7Cline%7Canchor&sections=all";
        String referer = "https://en.m.wikipedia.org/wiki/Tropical_cyclone";
        String userAgent = "Mozilla/5.0%20(Linux;%20Android%204.1.1;%20DROID%20RAZR%20HD%20Build/9.8.1Q_39)%20AppleWebKit/535.19%20(KHTML,%20like%20Gecko)%20Chrome/18.0.1025.166%20Mobile%20Safari/535.19";
        String statusCode = "200";
        String ipAddress = "127.0.0.1";
        String mimeType = "application/json; charset=utf-8";
        String requestMethod = "GET";

        Pageview pageview =  new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertEquals(PageviewType.API, PageviewType.determinePageviewType(new URL(url)));
        assertTrue(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }

    @Test
    public void testRefererPageview2() {
        String url = "https://en.m.wikipedia.org/w/api.php?format=json&action=query&prop=langlinks&llurl=true&lllimit=max&titles=Tropical+cyclone";
        String referer = "https://en.m.wikipedia.org/wiki/Tropical_cyclone";
        String userAgent = "Mozilla/5.0%20(Linux;%20Android%204.1.1;%20DROID%20RAZR%20HD%20Build/9.8.1Q_39)%20AppleWebKit/535.19%20(KHTML,%20like%20Gecko)%20Chrome/18.0.1025.166%20Mobile%20Safari/535.19";
        String statusCode = "200";
        String ipAddress = "127.0.0.1";
        String mimeType = "application/json; charset=utf-8";
        String requestMethod = "GET";

        Pageview pageview =  new Pageview(url, referer, userAgent, statusCode, ipAddress, mimeType, requestMethod);
        assertTrue(pageview.isPageview());
        assertFalse(pageview.isWebstatscollectorPageview());
        assertTrue(pageview.isWikistatsMobileReportPageview());
    }
}
