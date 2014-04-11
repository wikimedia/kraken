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

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CidrTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void testTrueSingleIpAddress() throws IOException {
        Tuple input = tupleFactory.newTuple(1);
        Tuple output;

        String subnet = "127.0.0.1/32";
        String ipAddress = "127.0.0.1";

        input.set(0, ipAddress);

        Cidr cidr = new Cidr(subnet);
        output = cidr.exec(input);
        assertEquals(output.get(0).toString(), "true");
    }

    @Test
    public void testFalseSingleIpAddress() throws IOException {
        Tuple input = tupleFactory.newTuple(1);
        Tuple output;

        String subnet = "127.0.0.1/32";
        String ipAddress = "192.168.1.1";

        input.set(0, ipAddress);

        Cidr cidr = new Cidr(subnet);
        output = cidr.exec(input);
        assertEquals(output.get(0).toString(), "false");
    }
}
