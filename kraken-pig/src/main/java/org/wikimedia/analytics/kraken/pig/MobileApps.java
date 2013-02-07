/**
 *Copyright (C) 2012  Wikimedia Foundation
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
 *
 * @version $Id: $Id
 */

package org.wikimedia.analytics.kraken.pig;


import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;


/*
 * See http://www.mediawiki.org/wiki/Mobile/User_agents for the canonical list of
 * mobile app user agent strings.
 */
public class MobileApps extends EvalFunc<Tuple>{

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private Pattern Android = Pattern.compile("WikipediaMobile\\/\\d\\.\\d(\\.\\d)?");
    private Pattern Firefox = Pattern.compile(Pattern.quote("Mozilla/5.0%20(Mobile;%20rv:18.0)%20Gecko/18.0%20Firefox/18.0"));
    private Pattern RIM = Pattern.compile(Pattern.quote("Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+"));
    private Pattern Windows = Pattern.compile(Pattern.quote("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)"));

    public List<Pattern> patterns = new ArrayList<Pattern>(5);

    public MobileApps() {
        patterns.add(0, Firefox);
        patterns.add(1, Android);
        patterns.add(2, RIM);
        patterns.add(3, Windows);
    }

    private String unspace(String useragent) {
        return useragent.replace("%20", " ");
    }

    private boolean detectMobileApp(String useragent) {
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(useragent);
            if (matcher.matches()) {
                return true;
            }
        }
        return false;
    }

    private void tokenize(String useragent) {
        StringTokenizer tokenizer = new StringTokenizer(useragent, "/\\[],.() ");

    }

    @Override
    public Tuple exec(Tuple input) throws ExecException {
        if (input == null || input.size() != 1) {
            return null;
        }

        String useragentRaw = (String) input.get(0);
        String useragent = unspace(useragentRaw);
        Boolean result = detectMobileApp(useragent);

        Tuple output = tupleFactory.newTuple(1);
        return output;
    }

}
