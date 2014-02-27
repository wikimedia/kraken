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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Stores data into a snappy block-compressed sequence file.
 * <p>
 * Data to store is read line-by-line from stdin. Each line is treated as
 * separate value. Keys are increasing, starting by 0.
 */
public class Store {
    /**
     * Stores data into a snappy block-compressed sequence file.
     *
     * @param args
     *            the first item is used as filename for the destination
     *            sequence file
     * @throws IOException
     *             if whatever IO problem occurs
     */
    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            System.err.println("Usage: FILE\n" + "\n"
                    + "FILE - store stdin as sequence file into this file.");
            System.exit(1);
        }

        Path path = new Path(args[0]);
        System.err.println("Reading from stdin, storing as " + path);

        BufferedReader reader = new BufferedReader(new InputStreamReader(
                System.in));

        Configuration conf = new Configuration();
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(Text.class),
                SequenceFile.Writer.compression(
                        SequenceFile.CompressionType.BLOCK,
                        new org.apache.hadoop.io.compress.SnappyCodec()));

        try {
            String line;
            long key = 0;

            // We loop over the lines and append them to the writer.
            //
            // Note that we do not treat the line's trailing newline as part of
            // the line.
            while ((line = reader.readLine()) != null) {
                writer.append(new LongWritable(key++), new Text(line));
            }
        } finally {
            writer.close();
        }
    }
}
