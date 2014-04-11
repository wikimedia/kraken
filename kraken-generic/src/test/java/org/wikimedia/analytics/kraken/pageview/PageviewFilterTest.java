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
    public void testValidMimeType() throws MalformedURLException{
        URL url = new URL("http://en.m.wikipedia.org/wiki/Earth");
        assertTrue(pageviewFilter.isValidMimeType(url, "text/html"));
    }

    @Test
    public void testValidMimeType2() throws MalformedURLException {
        URL url = new URL("http://en.m.wikipedia.org/wiki/Earth");
        assertTrue(pageviewFilter.isValidMimeType(url, "text/html; charset=UTF-8"));
    }

    @Test
    public void testInvalidMimeType() throws MalformedURLException {
        URL url = new URL("http://upload.wikimedia.org/wikipedia/commons/thumb/d/d0/CC-BY-SA_icon.svg/100px-CC-BY-SA_icon.svg.png");
        assertTrue(pageviewFilter.isValidMimeType(url, "image/png"));
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
        assertTrue(pageviewFilter.isApiPageview(url, referer));
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
        assertFalse(pageviewFilter.refersToDifferentArticle(url, referer));
    }

    @Test
    public void testRefersToSameArticle2() throws MalformedURLException {
        URL url = new URL("https://en.m.wikipedia.org/w/api.php?format=json&action=mobileview&page=Tornado&variant=en&redirects=yes&prop=sections%7Ctext&noheadings=yes&sectionprop=level%7Cline%7Canchor&sections=all");
        URL referer = new URL("https://en.m.wikipedia.org/wiki/Tropical_cyclone");
        assertTrue(pageviewFilter.refersToDifferentArticle(url, referer));
    }
}
