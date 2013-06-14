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
