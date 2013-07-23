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
package org.wikimedia.analytics.dclassjni;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import dclass.dClass;



public class DclassWrapperTest {

    public final String dtree_path = "/usr/share/libdclass/openddr.dtree";
    @Test
    public void testDclassWrapper() {
        String userAgentSample = "Mozilla/5.0 (Linux; U; Android 2.2; en; HTC Aria A6380 Build/ERE27) AppleWebKit/540.13+ (KHTML, like Gecko) Version/3.1 Mobile Safari/524.15.0";
        dClass dw = new dClass(dtree_path);
        Map<String, String> c = dw.classify(userAgentSample);
        assertEquals("Vendor as expected", "HTC"  , c.get("vendor"));
        assertEquals("Model as expected" , "A6380", c.get("model"));
        assertEquals("device_os as expected","Android",c.get("device_os"));
        assertEquals("is_tablet as expected","false",c.get("is_tablet"));
        
    }

}
