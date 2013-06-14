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

package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class ZeroFilterFuncTest {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testInitZeroFilterFunc() {
        ZeroFilterFunc zero1 = new ZeroFilterFunc();
        assertNotNull(zero1);
        assertEquals("default", zero1.getMode());
        ZeroFilterFunc zero2 = new ZeroFilterFunc("default");
        assertNotNull(zero2);
        assertEquals("default", zero2.getMode());
        ZeroFilterFunc zero3 = new ZeroFilterFunc("legacy");
        assertNotNull(zero3);
        assertEquals("legacy", zero3.getMode());
    }

    @Test(expected=RuntimeException.class)
    public void testFailureInitZeroFilterFunc() {
        ZeroFilterFunc zero = new ZeroFilterFunc("foo");
    }

    @Test
    public void testZeroLegacy() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/Californication_(TV_series)");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testZeroLegacy2() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Red_pog.svg/8px-Red_pog.svg.png");
        input.set(1, "default.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertFalse(zero.exec(input));
    }

    @Test
    public void testZeroLegacy3() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://meta.wikimedia.org/wiki/Special:RecordImpression?result=hide&reason=empty&country=KE&userlang=en&project=wikipedia&db=enwiki&bucket=0&anonymous=true&device=desktop");
        input.set(1, "default.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertFalse(zero.exec(input));
    }

    @Test
    public void testZeroLegacy4() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.wikipedia.org/wiki/James_Ingram");
        input.set(1, "default.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertFalse(zero.exec(input));
    }

    @Test
    public void testZeroLegacy5() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram");
        input.set(1, "default.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testZeroLegacy6() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "default.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testCountZeroDomain() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero-orange-tunesia.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertFalse(zero.exec(input));
    }

    @Test
    public void testCountMobileDomain()  throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero-orange-tunesia.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testCountEitherMobileOrZeroDomainMobile() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero-saudi-telecom.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testCountEitherMobileOrZeroDomainZero() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero-saudi-telecom.tab");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testXCSZeroConfigLookup1() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero=612-03;");
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertFalse(zero.exec(input));

    }

    @Test
    public void testXCSZeroConfigLookup2() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero=612-03;");
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testXCSZeroConfigLookup3() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, null);
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertFalse(zero.exec(input));
    }

    @Test
    public void testXCSZeroConfigLookup4() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "-");
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertFalse(zero.exec(input));
    }

    @Test
    public void testXCSWithoutKey() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "250-99");
        ZeroFilterFunc zero = new ZeroFilterFunc("legacy");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testXCSWithoutKey2() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "250-99");
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testXCSWithoutLanguageCode() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "250-99");
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertTrue(zero.exec(input));
    }

    @Test
    public void testZeroRequestWitKeyWithoutSemiColon() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/The_Slave_Hunters");
        input.set(1, "zero=420-01");
        ZeroFilterFunc zero = new ZeroFilterFunc("default");
        assertTrue(zero.exec(input));
    }

}
