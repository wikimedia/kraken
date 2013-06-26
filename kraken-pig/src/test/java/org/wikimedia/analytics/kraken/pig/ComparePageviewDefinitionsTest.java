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
