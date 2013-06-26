
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

import static org.junit.Assert.assertEquals;

public class CanonicalizeArticleTitleTest {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple input;

    private Tuple output;

    private CanonicalizeArticleTitle canonicalizeArticleTitle = new CanonicalizeArticleTitle();

    @Test
    public void testSimpleTitle() throws IOException {
        input = tupleFactory.newTuple(1);
        input.set(0, "http://en.wikipedia.org/wiki/Conquistador");
        output = tupleFactory.newTuple(1);
        output = canonicalizeArticleTitle.exec(input);
        assertEquals("Conquistador", output.get(0));
    }

    @Test
    public void testIndexPhpTitle() throws IOException {
        input = tupleFactory.newTuple(1);
        input.set(0, "http://en.wikipedia.org/w/index.php?title=Wikipedia:Village_pump_(technical)");
        output = tupleFactory.newTuple(1);
        output = canonicalizeArticleTitle.exec(input);
        assertEquals("Wikipedia:Village_pump_(technical)", output.get(0));
    }


}
