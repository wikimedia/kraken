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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        assertTrue(pageviewFilter.isValidMimeType("text"));
    }

    @Test
    public void testInvalidMimeType() {
        assertTrue(pageviewFilter.isValidMimeType("image"));
    }

    @Test
    /**
     *  test r5 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testValidMobileDesktopView() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/wiki/Earth");
        assertTrue(pageviewFilter.isValidMobilePageview(url));
    }
    @Test
    /**
     *  test r6 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testInvalidMobileApiPageview() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/w/api.php");
        URL referer = new URL("http://en.m.wikipedia.org");
        assertFalse(pageviewFilter.isValidMobileAPIPageview(url, referer));
    }

    @Test
    /**
     *  test r7 in fast-field-parser-xs/PageViews-FieldParser/t/01-get-wikiproject-for-url.t
     */
    public void testInvalidMobilePageview() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/wiki");
        assertFalse(pageviewFilter.isValidMobilePageview(url));
    }
}

