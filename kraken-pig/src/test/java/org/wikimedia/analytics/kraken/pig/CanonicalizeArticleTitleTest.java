
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

import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public class CanonicalizeArticleTitleTest {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple input;

    private Tuple output;

    private CanonicalizeArticleTitle canonicalizeArticleTitle = new CanonicalizeArticleTitle();

    @Test
    public void testSemiColonInUri() throws URISyntaxException, MalformedURLException {
        // A bug in Apache HttpComponents 4.0.x means
        // that it is not properly handling semicolons to separate key/values
        // in a query String. The solution is to replace the semicolon with an
        // ampersand.

        String urlString = "http://m.heise.de/newsticker/meldung/TomTom-baut-um-1643641.html?mrw_channel=ho;mrw_channel=ho;from-classic=1";
        URL url = new URL(urlString.replace(";", "&"));
        URLEncodedUtils.parse(url.toURI(), "utf-8");
    }

    @Test
    public void test()  throws URISyntaxException, MalformedURLException {
        //URL url = new URL("http://en.wikipedia.org/w/index.php?search=symptoms+of+vitamin+d+deficiency&title=Special%3ASearch");
        URL url = new URL("http://en.wikipedia.org/w/index.php?");
        URLEncodedUtils.parse(url.toURI(), "utf-8");
    }

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
