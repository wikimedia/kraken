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


import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.*;

public class PageviewFilterTest {

    PageviewFilter pageviewFilter = new PageviewFilter();

    @Test
    public void testGoogleBotAsValidUserAgent() {
        assertFalse(pageviewFilter.isValidUserAgent("Mozilla/5.0%20(compatible;%20Googlebot/2.1;%20+http://www.google.com/bot.html)"));
    }

    @Test
    public void testSpaceInWikiTitle() {
        URL url;
        try {
             url = new URL("https://fr.wikipedia.org/wiki/Discussion:Histoire du Racing Club de Strasbourg");
        } catch (MalformedURLException e) {
             url = null;
        }
        assertNull(url.toString(), null);
    }

    @Test
    public void testRegularValidResponseCode() {
        assertTrue(pageviewFilter.isValidResponseCode("200"));
    }

    @Test
    public void testVarnishValidResponseCode() {
        assertTrue(pageviewFilter.isValidResponseCode("hit/200"));
    }

    @Test
    public void testRegularInvalidResponseCode() {
        assertFalse(pageviewFilter.isValidResponseCode("500"));
    }

    @Test
    public void testValidMimeType() {
        assertTrue(pageviewFilter.isValidMimeType(PageviewType.REGULAR, "text/html"));
    }

    @Test
    public void testValidMimeType2() {
        assertTrue(pageviewFilter.isValidMimeType(PageviewType.REGULAR, "text/html; charset=UTF-8"));
    }

    @Test
    public void testInvalidMimeType() {
        assertTrue(pageviewFilter.isValidMimeType(PageviewType.IMAGE, "image/png"));
    }

    @Test
    /**
     *  test r5 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testValidMobileDesktopView() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/wiki/Earth");
        assertTrue(pageviewFilter.isValidRegularPageview(url));
    }
    @Test
    /**
     *  test r6 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testInvalidMobileApiPageview() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/w/api.php");
        URL referer = new URL("http://en.m.wikipedia.org");
        PageviewType pageviewType = PageviewType.determinePageviewType(url);
        PageviewType pageviewTypeReferer = PageviewType.determinePageviewType(referer);
        assertTrue(pageviewFilter.isApiPageview(pageviewType, url, pageviewTypeReferer, referer));
    }

    @Test
    /**
     *  test r7 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testInvalidMobilePageview() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/wiki");
        assertFalse(pageviewFilter.isValidRegularPageview(url));
    }


    @Test
    /**
     * http://blog.wikimedia.org/wp-login.php
     * http://blog.wikimedia.org/wp-admin/
     * http://blog.wikimedia.org/?s=  (i.e. searches)
     * http://blog.wikimedia.org/?p=22448&preview=true
     */
    public void testInvalidBlogPageRequest() throws MalformedURLException {
        URL url1 = new URL("http://blog.wikimedia.org/wp-login.php");
        URL url2 = new URL("http://blog.wikimedia.org/wp-admin/");
        URL url3 = new URL("http://blog.wikimedia.org/?s=foo");
        URL url4 = new URL("http://blog.wikimedia.org/?p=22448&preview=true");
        assertFalse(pageviewFilter.isValidBlogPageview(null));
        assertFalse(pageviewFilter.isValidBlogPageview(url1));
        assertFalse(pageviewFilter.isValidBlogPageview(url2));
        assertFalse(pageviewFilter.isValidBlogPageview(url3));
        assertFalse(pageviewFilter.isValidBlogPageview(url4));
    }

    @Test
    public void tesValidBlogPageRequest() throws MalformedURLException {
        URL url = new URL("https://blog.wikimedia.org/2013/01/12/remembering-aaron-swartz-1986-2013/");
        assertTrue(pageviewFilter.isValidBlogPageview(url));
    }


    @Test
    public void testRefersToSameArticle1() throws MalformedURLException {
        URL url = new URL("https://en.m.wikipedia.org/w/api.php?format=json&action=query&prop=langlinks&llurl=true&lllimit=max&titles=Tropical+cyclone");
        URL referer = new URL("https://en.m.wikipedia.org/wiki/Tropical_cyclone");
        PageviewType pageviewTypeUrl = PageviewType.determinePageviewType(url);
        PageviewType pageviewTypeReferer = PageviewType.determinePageviewType(referer);
        assertFalse(pageviewFilter.refersToDifferentArticle(pageviewTypeUrl, url, pageviewTypeReferer, referer));
    }

    @Test
    public void testRefersToSameArticle2() throws MalformedURLException {
        URL url = new URL("https://en.m.wikipedia.org/w/api.php?format=json&action=mobileview&page=Tornado&variant=en&redirects=yes&prop=sections%7Ctext&noheadings=yes&sectionprop=level%7Cline%7Canchor&sections=all");
        URL referer = new URL("https://en.m.wikipedia.org/wiki/Tropical_cyclone");
        PageviewType pageviewTypeUrl = PageviewType.determinePageviewType(url);
        PageviewType pageviewTypeReferer = PageviewType.determinePageviewType(referer);
        assertTrue(pageviewFilter.refersToDifferentArticle(pageviewTypeUrl, url, pageviewTypeReferer, referer));
    }
}
