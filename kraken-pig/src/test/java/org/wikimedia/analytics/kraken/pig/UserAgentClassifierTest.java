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

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;


public class UserAgentClassifierTest {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testFirefoxWikimediaApp() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        input.set(0, "Mozilla/5.0 (Mobile; rv:18.0) Gecko/18.0 Firefox/18.0");
        Tuple output = ua.exec(input);
        assertEquals("Firefox OS", output.get(7));

        input.set(0, "starts with something weird Mozilla/5.0 (Mobile; rv:18.0) Gecko/18.0 Firefox/18.0");
        output = ua.exec(input);
        assertEquals("Firefox OS", output.get(7));
    }

    @Test
    public void testAndroidWikimediaApp() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        input.set(0, "WikipediaMobile/1.3.4%20Mozilla/5.0%20(Linux;%20U;%20Android%204.0.3;%20en-us;%20PG86100%20Build/IML74K)%20AppleWebKit/534.30%20(KHTML,%20like%20Gecko)%20Version/4.0%20Mobile%20Safari/534.30");
        Tuple output = ua.exec(input);
        assertEquals("Android", output.get(7));

        input.set(0, "starts with something weird WikipediaMobile/1.3.4%20Mozilla/5.0%20(Linux;%20U;%20Android%204.0.3;%20en-us;%20PG86100%20Build/IML74K)%20AppleWebKit/534.30%20(KHTML,%20like%20Gecko)%20Version/4.0%20Mobile%20Safari/534.30");
        output = ua.exec(input);
        assertEquals("Android", output.get(7));
    }

    @Ignore
    @Test
    public void testRIMWikimediaApp() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        input.set(0, "Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+");
        Tuple output = ua.exec(input);
        assertEquals("BlackBerry PlayBook", output.get(7));

        input.set(0, "starts with something weird Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+");
        output = ua.exec(input);
        assertEquals("BlackBerry PlayBook", output.get(7));
    }

    @Test
    public void testWindowsWikimediaApp() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        input.set(0, "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)");
        Tuple output = ua.exec(input);
        assertEquals("Windows 8", output.get(7));

        input.set(0, "starts with something weird Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)");
        output = ua.exec(input);
        assertEquals("Windows 8", output.get(7));
    }

    @Test
    public void testIOSWikimediaApp() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);

        input.set(0, "Mozilla/5.0 (iPhone; CPU iPhone OS 6_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10B142");
        Tuple output = ua.exec(input);
        assertEquals("iOS", output.get(7));

        input.set(0, "Mozilla/5.0 (iPhone; CPU Safari iPhone OS 6_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10B142");
        output = ua.exec(input);
        assertEquals(null, output.get(7));

        input.set(0, "Mozilla/5.0 (iPhone; CPU iPhone OS 6_1 like Mac OS X) Safari AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10B142");
        output = ua.exec(input);
        assertEquals(null, output.get(7));

        input.set(0, "starts with something weird Mozilla/5.0 (iPhone; CPU iPhone OS 6_1 like Mac OS X) Safari AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10B142");
        output = ua.exec(input);
        assertEquals(null, output.get(7));
    }

    @Test
    public void testIPhone5() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        input.set(0, "Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%206_0_1%20like%20Mac%20OS%20X)%20AppleWebKit/536.26%20(KHTML,%20like%20Gecko)%20Version/6.0%20Mobile/10A525%20Safari/8536.25");
        Tuple output = ua.exec(input);
        assertEquals("genericApple", output.get(0));
        assertEquals("iPhone OS", output.get(2));
//        assertEquals("iPhone 5 CDMA 6.0.1", output.get(2));
    }


    @Test
    public void testIPhone4() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        input.set(0, "Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%205_1_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9B206%20Safari/7534.48.3");
        Tuple output = ua.exec(input);
        assertEquals("genericApple", output.get(0));
        assertEquals("iPhone OS", output.get(2));
//        assertEquals("iPhone 4S 5.1.1", output.get(2));
    }

    @Test
    public void testUnknownIPhone() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        // This is a real iPhone user agent string and is not present in the ios.json file :(
        input.set(0, "Mozilla/5.0%20(iPhone;%20CPU%20iPhone%20OS%206_1_2%20like%20Mac%20OS%20X)%20AppleWebKit/536.26%20(KHTML,%20like%20Gecko)%20Version/6.0%20Mobile/10B146%20Safari/8536.25");
        Tuple output = ua.exec(input);
        assertEquals("genericApple", output.get(0));
        assertEquals("iPhone OS", output.get(2));
//        assertEquals(output.get(2), null
    }

    @Test
    public void testIpad2() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        // This is a real iPhone user agent string and is not present in the ios.json file :(
        input.set(0, "Mozilla/5.0%20(iPad;%20CPU%20OS%205_0_1%20like%20Mac%20OS%20X)%20AppleWebKit/534.46%20(KHTML,%20like%20Gecko)%20Version/5.1%20Mobile/9A405%20Safari/7534.48.3");
        Tuple output = ua.exec(input);
        assertEquals("genericApple", output.get(0));
        assertEquals("iPhone OS", output.get(2));
//        assertEquals("iPad 2 CDMA 5.0.1", output.get(2));
    }

    @Test
    public void testIpod() throws IOException, ParseException{
        UserAgentClassifier ua = new UserAgentClassifier();
        Tuple input =  tupleFactory.newTuple(1);
        // This is a real iPhone user agent string and is not present in the ios.json file :(
        input.set(0, "Mozilla/5.0%20(iPod;%20CPU%20iPhone%20OS%206_0_1%20like%20Mac%20OS%20X)%20AppleWebKit/536.26%20(KHTML,%20like%20Gecko)%20Version/6.0%20Mobile/10A523%20Safari/8536.25");
        Tuple output = ua.exec(input);
        assertEquals("genericApple", output.get(0));
        assertEquals("iPhone OS", output.get(2));
//        assertEquals("iPod touch 5th generation 6.0", output.get(2));
    }
}
