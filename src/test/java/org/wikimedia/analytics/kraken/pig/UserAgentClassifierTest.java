/**
 Copyright (C) 2012  Wikimedia Foundation

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;

import org.apache.pig.tools.parameters.ParseException;
import org.wikimedia.analytics.dclassjni.DclassWrapper;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.io.File;
import java.io.IOException;


public class UserAgentClassifierTest {
    @Test
    public void testDclassLoader() {
        DclassWrapper dw = new DclassWrapper();
        dw.initUA();
        assertNotNull(dw);
    }

    @Test
    public void testClassifier() throws IOException, ParseException{
        File file = new File("src/test/java/resources/dclass.pig");
        PigTest test = new PigTest(file.toString());
        String[] output1 = {"(Android,HTC,A6380,true,false)", "(-,desktop,browser,false,false)"};
        test.assertOutput("DEVICES", output1);

    }
}
