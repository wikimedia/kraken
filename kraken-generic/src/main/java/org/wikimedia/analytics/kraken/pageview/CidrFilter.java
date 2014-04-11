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
        /**
         * 10.x.x.x are internal IP addresses.
         */
        this.subnets.add(new SubnetUtils("10.0.0.0/8"));

        /**
         * 208.80.152.0/22 && 91.198.174.0/24 are internal WMF office ranges and should be excluded from
         * pageview counts
         */
        this.subnets.add(new SubnetUtils("208.80.152.0/22"));
        this.subnets.add(new SubnetUtils("91.198.174.0/24"));
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
        if (isIp4Address(ipAddress)) {
            for (SubnetUtils subnet : subnets) {
                if (subnet.getInfo().isInRange(ipAddress)) {
                    return true;
                }
            }
            return false;
        } else {
            //TODO: IPv6 Range checking is not yet implemented
            return false;
        }
    }

    /**
     *
     * @param ipAddress
     */
    private boolean isIp4Address(final String ipAddress) {
        return !ipAddress.contains(":");
    }
}
