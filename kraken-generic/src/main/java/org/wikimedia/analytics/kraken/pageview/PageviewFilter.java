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


import java.net.URL;

/**
 *
 */
public class PageviewFilter {

    /**
     *
     * @param userAgent
     * @return
     */
    public final boolean isValidUserAgent(final String userAgent) {
        //This should be replaced using the dClass device detector
        if (userAgent.contains("bot")
            || userAgent.contains("spider")
            || userAgent.contains("http")
            || userAgent.contains("crawler")) {
            return false;
        } else {
            return true;
        }
    }
    /**
     *
     * @param url
     * @return
     */
    public final boolean isValidDesktopPageview(final URL url) {
        if (url.getPath().contains("Special:")) {
            return false;
        } else if (url.getPath().contains("/wiki/")) {
            return true;
        }
        return false;
    }

    /**
     *
     * @param url
     * @return
     */
    public final boolean isValidMobilePageview(final URL url) {
        //TODO: not yet implemented
        return true;
    }

    /**
     *
     * @param url
     * @param referer
     * @return
     */
    public final boolean isValidMobileAPIPageview(final URL url, final URL referer) {
        //TODO: not yet implemented
        return true;
    }

    /**
     *
     * @param ipAddress
     * @return
     */
    public final boolean isInternalWMFTraffic(final String ipAddress) {
        //TODO: not yet implemented
        return true;
    }

    /**
     *
     * @param mimeType
     * @return
     */
    public final boolean isValidMimeType(final String mimeType) {
        //TODO: not yet implemented
        return true;
    }

    /**
     *
      * @param responseCode
     * @return
     */
    public final boolean isValidResponseCode(final String responseCode) {
        //TODO: not yet implemented
        return true;
    }
    /**
     *Ignore the following paths:
     * http://testblog.wikimedia.org
     * http://blog.wikimedia.org/wp-login.php
     * http://blog.wikimedia.org/wp-admin/
     * http://blog.wikimedia.org/?s=  (i.e. searches)
     * @param url
     * @return
     */
    public final boolean isValidBlogPageview(final URL url) {
        if (url != null && (
                url.getPath().startsWith("wp-")
                || url.getPath().startsWith("?s=")
                || url.getHost().startsWith("test"))) {
            return false;
        }
        return true;
    }


}
