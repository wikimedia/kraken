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


import com.google.common.net.MediaType;

import java.net.URL;



/**
 * The general Pageview filter class for all jobs running on Kraken.
 */
public class PageviewFilter {


    private PageviewCanonical pageviewCanonicalTitle;

    private PageviewCanonical pageviewCanonicalReferer;

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
     * @param referer
     */
    public final boolean callPageviewHandler(final URL url, final URL referer) {
        PageviewType pageviewType = PageviewType.determinePageviewType(url);
        switch (pageviewType) {
            case API:
                return isApiPageview(url, referer);

            case REGULAR:
                return isValidRegularPageview(url);

            default:
                return false;
        }
    }

    /**
     *
     * @param url
     * @return
     */
    public final boolean isValidRegularPageview(final URL url) {
        return (url.getPath().contains("/wiki/")
                || url.getPath().contains("/w/index.php"));
//        if (url.getPath().contains("/wiki/")
//                || url.getPath().contains("/w/index.php")) {
//            return true;
//        }
//        return false;
    }

    /**
     * @param url
     * @param referer
     * @return true/false
     */
    public final boolean isApiPageview(final URL url, final URL referer) {
        boolean resultMainRequest = isApiPageviewRequest(url);
        if (referer == null) {
            return resultMainRequest;
        } else {
            boolean resultRefererRequest = isApiPageviewRequest(referer);
            if (resultRefererRequest) {
                return refersToDifferentArticle(url, referer);
            } else {
                return true;
            }
        }
    }

    /**
     *
     * @param url
     * @return
     */
   private boolean isApiPageviewRequest(final URL url) {
        return (url.getPath().contains("/w/api.php")
                && url.getQuery() != null
                && (url.getQuery().contains("action=view")
                    || url.getQuery().contains("action=mobileview")
                    || url.getQuery().contains("action=query")));
    }



    /**
     *
     * @param url
     * @param referer
     * @return
     */
    public final boolean refersToDifferentArticle(final URL url, final URL referer) {
        pageviewCanonicalTitle = new PageviewCanonical(url);
        pageviewCanonicalReferer = new PageviewCanonical(referer);
        pageviewCanonicalTitle.canonicalize("default");
        pageviewCanonicalReferer.canonicalize("default");

        if (pageviewCanonicalTitle.getArticleTitle() == null || pageviewCanonicalReferer.getArticleTitle() == null) {
            return true;
        } else {
            return pageviewCanonicalTitle.getArticleTitle().equals(pageviewCanonicalReferer.getArticleTitle()) ? false : true;
        }
    }

    /**
     *
     * @param url
     * @param mimeType
     * @return
     */
    public final boolean isValidMimeType(final URL url, final String mimeType) {
        PageviewType pageviewType = PageviewType.determinePageviewType(url);
        MediaType mediaType;
        try {
             mediaType = MediaType.parse(mimeType);
        } catch (java.lang.IllegalArgumentException e) {
            return false;
        }

        switch (pageviewType) {
            case IMAGE:
                return (mediaType.type().contains("image"));

            case API:
                return (mediaType.type().equals("application") && mediaType.subtype().equals("json"));

            case REGULAR:
                return (mediaType.type().equals("text") && ((mediaType.subtype().equals("html"))
                || mediaType.subtype().equals("vnd.wap.wml")));

            default:
                return (mediaType.type().equals("text") && (mediaType.subtype().equals("html")));
        }
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
        return (requestMethod.contains("get"));
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
