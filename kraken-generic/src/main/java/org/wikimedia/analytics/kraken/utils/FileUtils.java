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

package org.wikimedia.analytics.kraken.utils;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class FileUtils {
    public static String unCompressGzipFile(String path) {
        System.out.println(path);
        File file = new File(path);
        byte[] buffer = new byte[4096];
        GZIPInputStream gzip;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            gzip = new GZIPInputStream(new FileInputStream(file.toString()));
            int len;
            while ((len = gzip.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            gzip.close();
            baos.close();
        } catch (FileNotFoundException e) {
            System.err.println("Input file " + path
                    + " does not exist.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String data = "";
        try {
            data = baos.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            // This is always UTF-8 so this should never happen.
            e.printStackTrace();
        }
        return data;
    }
}
