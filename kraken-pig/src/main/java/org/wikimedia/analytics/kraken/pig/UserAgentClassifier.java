/**
 * Copyright (C) 2012-2013  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.dclassjni.DclassWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserAgentClassifier  extends EvalFunc<Tuple> {
        DclassWrapper dw = null;
        private String useragent = null;
        private Map result = new HashMap<String, String>();
        private List args = new ArrayList<String>();
        private final List knownArgs = new ArrayList<String>();

    private Pattern Android = Pattern.compile("WikipediaMobile\\/\\d\\.\\d(\\.\\d)?");
    private Pattern Firefox = Pattern.compile(Pattern.quote("Mozilla/5.0%20(Mobile;%20rv:18.0)%20Gecko/18.0%20Firefox/18.0"));
    private Pattern RIM = Pattern.compile(Pattern.quote("Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+"));
    private Pattern Windows = Pattern.compile(Pattern.quote("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)"));

    public List<Pattern> patterns = new ArrayList<Pattern>(5);

    public UserAgentClassifier() {
        if (this.dw == null) {
            this.dw = new DclassWrapper();
        }
        //knownArgs.add(0,);
        this.dw.initUA();
        patterns.add(0, Firefox);
        patterns.add(1, Android);
        patterns.add(2, RIM);
        patterns.add(3, Windows);

    }

    public UserAgentClassifier(String[] args) {
        this();
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


    @Override
    /**
     * {@inheritDoc}
     *
     * Method exec takes a tuple containing a single object:
     * 1) A tuple containing a useragent string
     */
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1 || input.get(0) == null) {
            return null;
        }


        this.useragent = input.get(0).toString();
        result = this.dw.classifyUA(this.useragent);

        //Create the output tuple
        Tuple output = TupleFactory.getInstance().newTuple(6);
        output.set(0, result.get("vendor"));
        output.set(1, result.get("device_os"));
        output.set(2, result.get("device_os_version"));
        output.set(3, convertToBoolean(result, "is_wireless_device"));
        output.set(4, convertToBoolean(result, "is_tablet"));
        output.set(5, result.get("uaprof"));

        return output;
    }

    private boolean convertToBoolean(Map<String, String> result, String param){
        return Boolean.parseBoolean(result.get(param));
    }

    private Tuple postProcessApple(Tuple output) throws ExecException{

        return output;

    }

    private Tuple postProcessSamsung(Tuple output) throws ExecException {
        /*
        This function takes a Samsung model (GT-S5750E, GT S5620) and drops all
        suffix characters and digits to allow for rollup of the keys.
         */

        String model = (String) result.get("model");
        Matcher m = this.Android.matcher(model);
        if (m.matches() && m.groupCount() == 4) {
            String name = m.group(1);
            String value = m.group(3);
            String valueCleaned = value.replaceAll("\\d","");
            String modelCleaned = name + "-" + valueCleaned;
            output.set(2, modelCleaned);
        } else {
            output.set(2, m.group(0));
        }

        return output;

    }

    public void finalize() {
      this.dw.destroyUA();
    }
}
