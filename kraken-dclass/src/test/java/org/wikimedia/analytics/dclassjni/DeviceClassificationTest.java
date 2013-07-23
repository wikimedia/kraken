package org.wikimedia.analytics.dclassjni;

import static org.junit.Assert.*;

import org.junit.Test;

import org.wikimedia.analytics.dclassjni.DeviceClassification;

public class DeviceClassificationTest {

    @Test
    public void testDeviceClassification() {
        String userAgentSample = "Mozilla/5.0 (Linux; U; Android 2.2; en; HTC Aria A6380 Build/ERE27) AppleWebKit/540.13+ (KHTML, like Gecko) Version/3.1 Mobile Safari/524.15.0";
        DeviceClassification d = new DeviceClassification();
        d.classifyUseragent(userAgentSample);

        assertEquals("Vendor as expected", "HTC"  , d.getVendor());
        assertEquals("Model as expected" , "A6380", d.getModel());
    }
}
