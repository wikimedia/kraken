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


import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PageViewFilterFuncTest {
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Test
    public void test1() throws ExecException {
        Tuple input =  tupleFactory.newTuple(7);
        PageViewFilterFunc page = new PageViewFilterFunc();
        input.set(0, "http://en.m.wikipedia.org/wiki/Main_Page");
        input.set(1, "-");
        input.set(2, "useragent");
        input.set(3, "200");
        input.set(4, "0.0.0.0");
        input.set(5, "text/html");
        input.set(6, "GET");
        assertTrue(page.exec(input));
    }

}
