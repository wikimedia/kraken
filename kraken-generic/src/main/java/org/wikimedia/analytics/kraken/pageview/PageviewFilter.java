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


import com.google.common.net.MediaType;

import java.net.URL;


/**
 * The general Pageview filter class for all jobs running on Kraken.
 */
public class PageviewFilter {


    /**
     *
     */
    public final boolean isNotBitsOrUploadDomain(final URL url) {
        if (url.getHost().contains("bits") || url.getHost().contains("upload")) {
            return false;
        } else {
            return true;
        }
    }

    /**
     *
     * @param userAgent string identifying the device/browser used by the visitor.
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
        //for now, the logic is the same but this is likely to change in the future
        return isValidDesktopPageview(url);
    }

    /**
     *
     * @param url
     * @param referer
     * @return
     */
    public final boolean isValidMobileAPIPageview(final URL url, final URL referer) {
        //Start with simple logic, if referer is another Wiki* api call then ignore this url else accept it
        if (referer.getPath().contains("api.php")
                && referer.getHost().contains(".wiki")
                && referer.getQuery() != null
                && referer.getQuery().contains("view")) {
            return false;
        } else {
            return true;
        }
    }

    /**
     *
     * @param pageviewType
     * @param mimeType
     * @return
     */
    public final boolean isValidMimeType(final PageviewType pageviewType, final String mimeType) {
        boolean result;
        if (pageviewType == PageviewType.COMMONS_IMAGE) {
            result = isValidCommonsImageMimeType(mimeType);
        } else {
            result = isValidPageviewMimeType(mimeType);
        }
        return result;
    }


    /**
     *
     * @param mimeType
     * @return
     */
    private boolean isValidPageviewMimeType(final String mimeType) {
        MediaType mediaType = MediaType.parse(mimeType);
        if ((mediaType.type().equals("text") && (mediaType.subtype().equals("html"))
                || (mediaType.type().equals("application") && mediaType.subtype().equals("json")))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * @param mimeType
     * @return
     */
    private boolean isValidCommonsImageMimeType(final String mimeType) {
        MediaType mediaType = MediaType.parse(mimeType);
        if (mediaType.type().contains("image")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * @param responseCode
     * @return
     */
    public final boolean isValidResponseCode(final String responseCode) {
        if (responseCode.matches(".*(20\\d|302).*")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * @param requestMethod
     * @return
     */
    public final boolean isValidRequestMethod(final String requestMethod) {
        if (requestMethod.toLowerCase().contains("get")) {
            return true;
        } else {
            return false;
        }
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
