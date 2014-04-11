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

import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class LocalWebRequestTestFile {

    public static String[] load(final String fileName) throws IOException {
        InputStream inputStream =
                ComparePageviewDefinitionsTest.class.getResourceAsStream(fileName);
        InputStreamReader reader = new InputStreamReader(inputStream);
        LineReader lineReader = new LineReader(reader);

        ArrayList<String> logLines = new ArrayList<String>();

        while(true) {
            String logLine = lineReader.readLine();
            if (logLine != null) {
                logLines.add(logLine + "\\n");
            } else {
                break;
            }
        }

        String[] input = new String[logLines.size()];
        return logLines.toArray(input);
    }
}
