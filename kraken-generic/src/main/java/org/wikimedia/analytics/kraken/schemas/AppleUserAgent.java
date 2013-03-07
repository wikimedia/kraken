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

package org.wikimedia.analytics.kraken.schemas;

/**
 * This class provides the mapping to the iso.json file in src/main/resources.
 * The json file has been scraped from enterpriseios.com and that site contains
 * a detailed overview of Apple iPhone, iPod and iPad build numbers, iOS versions
 * and we use this as extra postprocessing step during device detection.
 * The guaranteed unique field in this class is the 'Build' field and
 * this field should be used as key.
 */
public class AppleUserAgent extends Schema {
    /**
     * The properties need to be public so that the JsonToClassConvert class can read them.
     */
    public String Product;
    public String AppleProduct;
    public String UserAgentPrefix;
    public String Build;
    public String Introduced;
    public String IOSVersion;

    public String getProduct() {
        return Product;
    }

    public String getAppleProduct() {
        return AppleProduct;
    }

    public String getUserAgentPrefix() {
        return UserAgentPrefix;
    }

    public String getBuild() {
        return Build;
    }

    public String getIntroduced() {
        return Introduced;
    }

    public String getIOSVersion() {
        return IOSVersion;
    }

    public String toString() {
        return getAppleProduct() + " " + getIOSVersion();
    }

}
