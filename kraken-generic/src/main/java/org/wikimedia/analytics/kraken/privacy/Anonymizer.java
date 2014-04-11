/**
 * Copyright (C) 2012  Wikimedia Foundation

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
