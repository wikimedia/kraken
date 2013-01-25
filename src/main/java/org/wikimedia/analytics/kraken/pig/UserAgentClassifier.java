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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.dclassjni.DclassWrapper;



import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class UserAgentClassifier  extends EvalFunc<Tuple> {
        DclassWrapper dw = null;
        private String useragent = null;
        private Map result = new HashMap<String, String>();
        private List args = new ArrayList<String>();
        private final List knownArgs = new ArrayList<String>();

    public UserAgentClassifier() {
        if (this.dw == null) {
            this.dw = new DclassWrapper();
        }
        //knownArgs.add(0,);
        this.dw.initUA();
    }

    public UserAgentClassifier(String[] args) {
        this();
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
        Tuple output = TupleFactory.getInstance().newTuple(5);
        output.set(0, result.get("device_os"));
        output.set(1, result.get("vendor"));
        output.set(2, result.get("model"));
        output.set(3, result.get("is_wireless_device"));
        output.set(4, result.get("is_tablet"));

        return output;
    }

    public void finalize() {
        this.dw.destroyUA();
    }
}
