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


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.pageview.PageviewCanonical;
import org.wikimedia.analytics.kraken.pageview.ProjectInfo;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 */
public class CanonicalizeArticleTitle extends EvalFunc<Tuple> {
    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple output;

    private URL url;

    private PageviewCanonical pageviewCanonical;

    private String mode;

    /**
     *
     */
    public CanonicalizeArticleTitle() {
        this.mode = "default";
    }

    /**
     *
     * @param mode
     */
    public CanonicalizeArticleTitle(String mode) {
        // Acceptable values are "default" and "webstatscollector"
        this.mode = mode;
    }


    @Override
    public Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.get(0) == null) {
            return null;
        }

        output = tupleFactory.newTuple(3);
        ProjectInfo projectInfo;
        try {
            url = new URL((String) input.get(0));
            pageviewCanonical = new PageviewCanonical(url);
            pageviewCanonical.canonicalize(mode);
            projectInfo = new ProjectInfo(url.getHost());

        } catch (MalformedURLException e) {
            return null;
        }
        output.set(0, pageviewCanonical.getArticleTitle());
        output.set(1, projectInfo.getLanguage());
        output.set(2, projectInfo.getProjectDomain());
        return output;
    }
}
