/**
 * Copyright (C) 2012  Wikimedia Foundation

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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashCode;


public class Session extends EvalFunc<Tuple> {


    int epoch = (int) (System.currentTimeMillis() / 1000);
    HashFunction hf = Hashing.murmur3_128(epoch);


    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() != 2) {
            return null;
        }
        DataBag bag = (DataBag) input.get(0);
        Iterator<Tuple> it = bag.iterator();
        Tuple ipAddress = (Tuple) input.get(1);
        Tuple userAgent = (Tuple) input.get(2);
        String sessionId;
        //Create the output tuple
        Tuple output = TupleFactory.getInstance().newTuple(1);

        while (it.hasNext()) {
            sessionId = generateId(ipAddress, userAgent);
            output.set(0, sessionId);
        }

        return output;
    }

    private String generateId(Tuple ipAddress, Tuple userAgent){
        HashCode hc = hf.newHasher().
                putString(ipAddress.toString()).
                putString(userAgent.toString()).
                hash();
        return hc.toString();
    }
}
