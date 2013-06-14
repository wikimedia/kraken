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

import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import org.wikimedia.analytics.kraken.zero.ZeroConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class ZeroConfigTest {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void initZeroConfigDefaultValue() {
        ZeroFilterFunc filter = new ZeroFilterFunc();
        ZeroConfig zeroConfig = filter.getZeroConfig("foo");
        assertNotNull(zeroConfig);
        assertEquals("default", zeroConfig.getCarrier());
    }

    @Test
    public void initZeroConfig() {
        ZeroFilterFunc filter = new ZeroFilterFunc();
        ZeroConfig zeroConfig = filter.getZeroConfig("zero-orange-kenya");
        assertNotNull(zeroConfig);
        assertEquals("Orange", zeroConfig.getCarrier());
        assertEquals("Kenya", zeroConfig.getCountry());
    }


}
