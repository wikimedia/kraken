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
    public void testXCSZeroConfigLookup1() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero=612-03;");
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertFalse(zero.exec(input));

    }

    @Test
    public void testXCSZeroConfigLookup2() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "zero=612-03;");
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertTrue(zero.exec(input));
    }

    @Test
    public void testXCSZeroConfigLookup3() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, null);
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertFalse(zero.exec(input));
    }

    @Test
    public void testXCSZeroConfigLookup4() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "-");
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertFalse(zero.exec(input));
    }

    @Test
    public void testXCSWithoutKey2() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "250-99");
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertTrue(zero.exec(input));
    }

    @Test
    public void testXCSWithoutLanguageCode() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://m.wikipedia.org/wiki/James_Ingram");
        input.set(1, "250-99");
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertTrue(zero.exec(input));
    }

    @Test
    public void testZeroRequestWitKeyWithoutSemiColon() throws IOException {
        Tuple input = tupleFactory.newTuple(2);
        input.set(0, "http://en.m.wikipedia.org/wiki/The_Slave_Hunters");
        input.set(1, "zero=420-01");
        ZeroFilterFunc zero = new ZeroFilterFunc();
        assertTrue(zero.exec(input));
    }

}
