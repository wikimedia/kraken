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


import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PageViewFilterFuncTest {
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void test1() throws ExecException {
        Tuple input =  tupleFactory.newTuple(7);
        PageViewFilterFunc page = new PageViewFilterFunc();
        input.set(0, "http://en.m.wikipedia.org/wiki/Main_Page");
        input.set(1, "-");
        input.set(2, "useragent");
        input.set(3, "200");
        input.set(4, "0.0.0.0");
        input.set(5, "text/html");
        input.set(6, "GET");
        assertTrue(page.exec(input));
    }

}
