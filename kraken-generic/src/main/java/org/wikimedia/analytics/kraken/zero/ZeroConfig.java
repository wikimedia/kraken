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
package org.wikimedia.analytics.kraken.zero;


import java.util.Calendar;

/**
 *
 */
public class ZeroConfig {

    private String country;
    private String carrier;
    private Calendar startDate;
    private boolean mobileDomainFree;
    private boolean zeroDomainFree;
    private String[] languages;
    private String[] restrictions;

    /**
     *
     * @param country
     * @param carrier
     * @param startDate
     * @param mobileDomainFree
     * @param zeroDomainFree
     * @param languages
     * @param restrictions
     */
    public ZeroConfig(final String country, final String carrier, final Calendar startDate, final boolean mobileDomainFree,
                      final boolean zeroDomainFree, final String[] languages, final String[] restrictions) {
        setCountry(country);
        setCarrier(carrier);
        setStartDate(startDate);
        setMobileDomainFree(mobileDomainFree);
        setZeroDomainFree(zeroDomainFree);
        setLanguages(languages);
        setRestrictions(restrictions);
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(final String country) {
        this.country = country;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(final String carrier) {
        this.carrier = carrier;
    }

    public Calendar getStartDate() {
        return startDate;
    }

    public void setStartDate(final Calendar startDate) {
        this.startDate = startDate;
    }

    public boolean isMobileDomainFree() {
        return mobileDomainFree;
    }

    public void setMobileDomainFree(final boolean mobileDomainFree) {
        this.mobileDomainFree = mobileDomainFree;
    }

    public boolean isZeroDomainFree() {
        return zeroDomainFree;
    }

    public void setZeroDomainFree(final boolean zeroDomainFree) {
        this.zeroDomainFree = zeroDomainFree;
    }

    public String[] getLanguages() {
        return languages;
    }

    public void setLanguages(final String[] languages) {
        this.languages = languages;
    }

    public String[] getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(final String[] restrictions) {
        this.restrictions = restrictions;
    }
}
