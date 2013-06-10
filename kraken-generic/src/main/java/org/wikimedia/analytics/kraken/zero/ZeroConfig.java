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
package org.wikimedia.analytics.kraken.zero;


import java.util.Calendar;
import java.util.Date;

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
