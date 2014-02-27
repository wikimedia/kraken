// Copyright (C) 2014 Wikimedia Foundation
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

package org.wikimedia.analytics.kraken.toolbelt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

/**
 * Dumps a sequence file to stdout.
 * <p>
 * Each value of the sequence file is printed on a separate line.
 */
public class Dump {
    /**
     * Dumps a sequence file and dumps it to stdout.
     * @param args the first item is used as name of the file be read.
     * @throws IOException if whatever IO problem occurs.
     * @throws IllegalAccessException if instantiating Writables fails.
     * @throws InstantiationException if instantiating Writables fails.
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {
        if (args == null || args.length != 1) {
            System.err.println("Usage: FILE\n" + "\n"
                    + "FILE - store stdin as sequence file into this file.");
            System.exit(1);
        }

        Path path = new Path(args[0]);

        Configuration conf = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path));
        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
                String valueStr = value.toString();
                if (valueStr.endsWith("\n")) {
                    System.out.print(valueStr);
                } else {
                    System.out.println(valueStr);
                }
            }
        } finally {
            reader.close();
        }
    }
}
