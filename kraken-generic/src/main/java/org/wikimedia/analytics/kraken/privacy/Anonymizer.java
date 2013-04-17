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
package org.wikimedia.analytics.kraken.privacy;


import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Anonymizer {

    private static final Map<String, HashFunction> HASH_FUNCTIONS = new HashMap<String, HashFunction>();
    static {
        HASH_FUNCTIONS.put("md5", Hashing.md5());
        HASH_FUNCTIONS.put("sha1", Hashing.sha1());
        HASH_FUNCTIONS.put("sha256", Hashing.sha256());
        HASH_FUNCTIONS.put("sha512", Hashing.sha512());
        HASH_FUNCTIONS.put("murmur3_32", Hashing.murmur3_32());
        HASH_FUNCTIONS.put("murmur3_128", Hashing.murmur3_128());
    }

    private HashFunction hf;
    private Hasher hasher;

    /**
     *
     * @param hashFunction
     */
    public Anonymizer(final String hashFunction) {
        hf = HASH_FUNCTIONS.get(hashFunction);
        hasher = hf.newHasher();
    }

    /**
     *
     * @param ipAddress
     * @param userAgent
     * @return
     */
    public final String generateHash(final String ipAddress, final String userAgent) {
        HashCode hc = this.hasher.
            putString(ipAddress).
            putString(userAgent).
            hash();
        return hc.toString();
    }
}
