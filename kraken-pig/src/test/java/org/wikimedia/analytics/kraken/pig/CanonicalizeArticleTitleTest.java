
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
