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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;

/**
 * Dumps information about a sequence file to stdout.
 */
public class Info {

    /**
     * Renders a row of key, and value for the user
     * @param key
     * @param value
     */
    private static void renderKeyValue(String key, String value) {
        System.out.println(key + ": " + value);
    }

    /**
     * Dumps information about a sequence file to stdout.
     *
     * @param args the first item is the filename of the file to get
     *     information about.
     * @throws IOException if file cannot be opened, read, ...
     */
    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            System.err.println("Usage: FILE\n"
                    + "\n"
                    + "FILE - file to get info about.");
            System.exit(1);
        }
        Path path = new Path(args[0]);
        Configuration conf = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path));

        String compressionType = reader.getCompressionType().toString();
        renderKeyValue("CompressionType", compressionType);

        String codecName = reader.getCompressionCodec().getClass().getName();
        String codecExt = reader.getCompressionCodec().getDefaultExtension();
        renderKeyValue("CompressionCodec", codecName + " (" + codecExt + ")");


        renderKeyValue("Key", reader.getKeyClassName());
        renderKeyValue("Value", reader.getValueClassName());

        Metadata metadata = reader.getMetadata();
        int metadataSize = metadata.getMetadata().size();
        renderKeyValue("Metadata", "(" + metadataSize + " metadata entries)");
        for (Entry<Text, Text> entry : metadata.getMetadata().entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            renderKeyValue(" * " + key, value);
        }

        reader.close();
    }
}
