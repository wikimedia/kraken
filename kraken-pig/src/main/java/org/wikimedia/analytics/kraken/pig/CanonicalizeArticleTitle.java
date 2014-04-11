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
