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
    /** A regular mobile pageview */
    MOBILE,

    /** A mobile pageview requested through the api */
    MOBILE_API,

    /** A mobile search request */
    MOBILE_SEARCH,

    /** A regular pageview */
    DESKTOP,

    /** A regular API request */
    API,

    /** A regular search request */
    SEARCH,

    /** A pageview on the blog */
    BLOG,

    /** An image from commons or upload */
    IMAGE,

    /** Other type of request */
    OTHER
    }


