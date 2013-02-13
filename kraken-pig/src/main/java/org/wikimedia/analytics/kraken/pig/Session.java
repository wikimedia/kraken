/**
 * Copyright (C) 2012  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;


/**
 * This class offers the functionality to create user sessions by hashing  the combination of useragent string,
 * and ip address.
 */
public class Session extends EvalFunc<Tuple> {

    private Hash hasher = new MurmurHash();
    private Configuration conf = new Configuration();
    private FileSystem fs;
    private int seed;


    /**
     * @param path path to hdfs that contains the seed value, this file should not be public.
     * @throws IOException
     */
    public Session(final String path) throws IOException {
        this.fs = FileSystem.get(conf);
        readSeedValue(path);
    }

    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    public final Tuple exec(final Tuple input) throws IOException {
        if (input == null || input.size() != 2) {
            return null;
        }
        String ipAddress = (String) input.get(1);
        String userAgent = (String) input.get(2);

        Tuple output = TupleFactory.getInstance().newTuple(1);


        output.set(0, generateId(ipAddress, userAgent));
        return output;
    }

    /**
     * Given a path to HDFS, read the contents of the file and read the contents as seed for the hashing function.
     * @param path path on HDFS to seed file
     * @throws IOException
     */
    private void readSeedValue(final String path) throws IOException {
        Path inFile = new Path(path);
        if (!fs.exists(inFile))
            System.err.println("Input file not found");
        if (!fs.isFile(inFile))
            System.err.println("Input should be a file");

        Charset cs = Charset.forName("utf-8");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile), cs));
        StringBuilder sb = new StringBuilder();
        String line;

        while ((line = br.readLine()) != null){
            System.out.println(line);
            sb.append(line);
        }
        br.close();

        int x = 0;
        for (int i = 0; i < sb.length(); i++){
            char c = sb.charAt(i);
            x = x + c;
        }
        this.seed = x;
    }

    /**
     * Given an ipAddress and a useragent string, generate an id of the combined byte array.
     * @param ipAddress the ipAddress from the logline
     * @param userAgent the userAgent string from the logline
     * @return the seed value as an integer.
     */
    public final int generateId(final String ipAddress, final String userAgent) {
        byte[] ipAddressBytes = ipAddress.getBytes();
        byte[] userAgentBytes = userAgent.getBytes();
        byte[] sessionInput = concat(ipAddressBytes, userAgentBytes);
        return this.hasher.hash(sessionInput, this.seed);
    }

    /**
     * Merge two arbitrary byte arrays
     * @param a byte array 1
     * @param b byte array 2
     * @return merged byte array
     */
    private byte[] concat(final byte[] a, final byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

//        HashCode hc = hf.newHasher().
//                putString(ipAddress.toString()).
//                putString(userAgent.toString()).
//                hash();
//        return hc.toString();

}
