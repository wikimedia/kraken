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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.wikimedia.analytics.kraken.privacy.Anonymizer;

import java.io.IOException;


/**
 * This class offers the functionality to create user sessions by hashing the combination of useragent string,
 * and ip address.
 */
public class Session extends EvalFunc<Tuple> {

    private Anonymizer anonymous;

    /**
     *
     */
    public Session(final String hashFunction) {
        anonymous = new Anonymizer(hashFunction);
    }

    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() != 2) {
            return null;
        }
        String ipAddress = (String) input.get(1);
        String userAgent = (String) input.get(2);

        Tuple output = TupleFactory.getInstance().newTuple(1);


        output.set(0, anonymous.generateHash(ipAddress, userAgent));
        return output;
    }
}
