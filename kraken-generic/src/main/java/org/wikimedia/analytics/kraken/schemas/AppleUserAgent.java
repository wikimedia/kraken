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
