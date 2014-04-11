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


import java.net.URL;

/**
 * This clas defines the different types of pageviews on the Wikimedia properties.
 */
public enum PageviewType {
    /** A regular api.php call. */
    API,

    /** A regular pageview. */
    REGULAR,

    /** A pageview on the blog. */
    BLOG,

    /** An image from the commons or upload domain. */
    IMAGE,

    /** A banner served from meta. */
    BANNER,

    /** A search request. */
    SEARCH,

    /** Other type of request. */
    OTHER,

    /** Not a valid webrequest. */
    NONE;

    /**
     * Given a url, determine the pageview type (mobile, desktop, api, search and blog).
     */
    public static final PageviewType determinePageviewType(final URL url) {
        if (url != null) {
            if (url.getQuery() != null && url.getQuery().contains("BannerLoader")) {
                return PageviewType.BANNER;
            } else if (url.getPath().contains("api.php")) {
                return PageviewType.API;
            } else if (url.getPath().contains("/wiki/")) {
                return PageviewType.REGULAR;
            } else if (url.getPath().contains("/w/index.php")) {
                return PageviewType.REGULAR;
            } else if (url.getHost().contains("upload") && url.getPath().contains("thumb")) {
                //TODO: More image formats need to be supported but there are no use cases yet.
                return PageviewType.IMAGE;
            } else if (url.getHost().contains("wikimediafoundation") && url.getPath().contains("blog")) {
                return PageviewType.BLOG;
            } else {
                return PageviewType.OTHER;
            }
        } else {
            return PageviewType.NONE;
        }
    }
}
