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


import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit tests using the PigUnit library. It is very important to remember:
 * 1) To test a UDF, you do not need to register the UDF in the pig script; just use org.package.class.function()
 * 2) Make sure that the inputAlias and the outputAias in the assertOutput() function having matching variable
 * names in the pig script.
 */
public class ComparePageviewDefinitionsTest {

    private static final String PIG_SCRIPT = "src/test/resources/compare_pageviews.pig";

    @Test
    public void testDesktopPageviewCount() throws IOException, ParseException {
        PigTest test = new PigTest(PIG_SCRIPT);

        String fileName = "/testdata_desktop.csv";

        String[] input = LocalWebRequestTestFile.load(fileName);

        String[] output = {
                "(1,5,2)"
        };

        test.assertOutput("log_fields", input, "grouped_counts", output);
    }


    @Test
    public void testMobilePageviewCount() throws IOException, ParseException {
        PigTest test = new PigTest(PIG_SCRIPT);

        String fileName = "/testdata_mobile.csv";

        String[] input = LocalWebRequestTestFile.load(fileName);

        String[] output = {
                "(14,16,18)"
        };

        test.assertOutput("log_fields", input, "grouped_counts", output);
    }
}
