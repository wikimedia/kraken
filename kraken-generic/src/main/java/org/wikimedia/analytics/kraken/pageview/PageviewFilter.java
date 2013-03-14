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
        return !(url.getHost().contains("bits") || url.getHost().contains("upload"));
    }

    /**
     *
     * @param userAgent string identifying the device/browser used by the visitor.
     * @return
     */
    public final boolean isValidUserAgent(final String userAgent) {
        //This should be replaced using the dClass device detector
        return !(userAgent.contains("bot")
                || userAgent.contains("spider")
                || userAgent.contains("http")
                || userAgent.contains("crawler"));
    }
    /**
     *
     * @param url
     * @return
     */
    public final boolean isValidDesktopPageview(final URL url) {
        if (url.getPath().contains("Special:")) {
            return false;
        } else if (url.getPath().contains("wiki/")
                || url.getPath().contains("w/index.php?")
                || url.getPath().contains("w/api.php?")) {
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
        return !(referer != null
                && referer.getPath().contains("api.php")
                && referer.getHost().contains(".wiki")
                && referer.getQuery() != null
                && referer.getQuery().contains("view"));
    }

    /**
     *
     * @param pageviewType
     * @param mimeType
     * @return
     */
    public final boolean isValidMimeType(final PageviewType pageviewType, final String mimeType) {
        switch (pageviewType) {

            case COMMONS_IMAGE:
                return isValidCommonsImageMimeType(mimeType);

            case MOBILE:
            case MOBILE_API:
            case MOBILE_SEARCH:
            case MOBILE_ZERO:
                return isValidMobilePageviewMimeType(mimeType);

            default:
                return isValidDesktopPageviewMimeType(mimeType);
        }
    }

    /**
     *
     * @param mimeType
     * @return
     */
    private boolean isValidMobilePageviewMimeType(final String mimeType) {
        MediaType mediaType = MediaType.parse(mimeType);
        return ((mediaType.type().equals("text")
                && ((mediaType.subtype().equals("html") || mediaType.subtype().equals("vnd.wap.wml")))
                || (mediaType.type().equals("application") && mediaType.subtype().equals("json"))));
    }


    /**
     *
     * @param mimeType
     * @return
     */
    private boolean isValidDesktopPageviewMimeType(final String mimeType) {
        MediaType mediaType = MediaType.parse(mimeType);
        return ((mediaType.type().equals("text") && (mediaType.subtype().equals("html"))
                || (mediaType.type().equals("application") && mediaType.subtype().equals("json"))));
    }

    /**
     *
     * @param mimeType
     * @return
     */
    private boolean isValidCommonsImageMimeType(final String mimeType) {
        MediaType mediaType = MediaType.parse(mimeType);
        return (mediaType.type().contains("image"));
    }

    /**
     *
     * @param responseCode
     * @return
     */
    public final boolean isValidResponseCode(final String responseCode) {
        return (responseCode.matches(".*(20\\d|302|304).*"));
    }

    /**
     *
     * @param requestMethod
     * @return
     */
    public final boolean isValidRequestMethod(final String requestMethod) {
        return (requestMethod.toUpperCase().contains("GET"));
    }

    /**
     *Ignore the following paths:
     * http://testblog.wikimedia.org
     * http://blog.wikimedia.org/wp-login.php
     * http://blog.wikimedia.org/wp-admin/
     * http://blog.wikimedia.org/?s=  (i.e. searches)
     * http://blog.wikimedia.org/?p=22448&preview=true (preview of article while editing)
     * @param url
     * @return
     */
    public final boolean isValidBlogPageview(final URL url) {
        if (url != null
                && url.getQuery() != null
                && (url.getQuery().startsWith("s=")
                || url.getQuery().contains("preview=true"))) {
            return false;
        } else {
            return (url != null
                && !url.getPath().startsWith("/wp-")
                && !url.getHost().startsWith("test"));
        }
    }
}
