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
