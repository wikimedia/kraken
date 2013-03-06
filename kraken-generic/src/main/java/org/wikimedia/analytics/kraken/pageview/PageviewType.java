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

/**
 * This clas defines the different types of pageviews on the Wikimedia properties.
 */
public enum PageviewType {
    /** A regular mobile pageview, url contains .m. but not api.php */
    MOBILE,

    /** A mobile pageview requested through the api, url contains .m. and also api.php */
    MOBILE_API,

    /** A mobile search request, url contains .m. and the string 'search' */
    MOBILE_SEARCH,

    /** A mobile zero request, url contains .zero. */
    MOBILE_ZERO,

    /** A regular pageview */
    DESKTOP,

    /** A regular API request */
    DESKTOP_API,

    /** A regular search request */
    DESKTOP_SEARCH,

    /** A pageview on the blog */
    BLOG,

    /** An image from commons or upload */
    COMMONS_IMAGE,

    /** A banner served from meta */
    BANNER,

    /** Other type of request */
    OTHER
    }
