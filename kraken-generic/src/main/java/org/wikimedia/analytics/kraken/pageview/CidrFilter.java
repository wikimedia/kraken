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


import org.apache.commons.net.util.SubnetUtils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class CidrFilter {
    /** a list containing all the CIDR ranges to check for */
    private List<SubnetUtils> subnets = new ArrayList<SubnetUtils>();


    /**
     * Default constructor that use WMF internal IP ranges
     */
    public CidrFilter() {
        //        "208.80.152.",
        //        "208.80.153."
        //        "208.80.154."
        //        "208.80.155."
        //        "91.198.174."
        this.subnets.add(new SubnetUtils("208.80.152.0/X"));
        this.subnets.add(new SubnetUtils("91.198.174.0/X"));
    }

    /**
     *
     * @param subnetInput a comma separated list of subnets using the CIDR notation.
     */
    public CidrFilter(final String subnetInput) {
        for (String subnet : subnetInput.split(",")) {
            SubnetUtils subnetUtil = new SubnetUtils(subnet);
            subnetUtil.setInclusiveHostCount(true);
            this.subnets.add(subnetUtil);
        }
    }

    /**
     *
     * @param ipAddress string that needs to be checked whether it falls in a given CIDR range.
     * @return true if ipAddress is within one of the specified CIDR ranges otherwise return false.
     */
    public final boolean ipAddressFallsInRange(final String ipAddress) {
        for (SubnetUtils subnet : subnets) {
            if (subnet.getInfo().isInRange(ipAddress)) {
                return true;
            }
        }
        return false;
    }
}
