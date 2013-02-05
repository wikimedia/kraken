/**
 Copyright (C) 2012  Wikimedia Foundation

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.wikimedia.analytics.kraken.pig;


public class Mcc_Mnc {

    public String Network;
    public String Country;
    public String MCC;
    public String CountryCode;
    public String ISO;
    public String Name;
    public String MCC_MNC;
    public String MNC;


    public String getNetwork() {
        return Network;
    }

    public String getCountry() {
        return Country;
    }

    public String getMCC() {
        return MCC;
    }

    public String getCountryCode() {
        return CountryCode;
    }

    public String getISO() {
        return ISO;
    }

    public String getName() {
        return Name;
    }

    public String getMCC_MNC() {
        return MCC_MNC;
    }

    public String getMNC() {
        return MNC;
    }
}
