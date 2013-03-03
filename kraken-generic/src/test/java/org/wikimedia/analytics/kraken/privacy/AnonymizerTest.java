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


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AnonymizerTest {

    @Test
    public void md5AnonymizerTest() {
        Anonymizer anonymous = new Anonymizer("md5");
        String hashCode = anonymous.generateHash("127.0.0.1", "fake user agent string");
        assertEquals(hashCode, "9c4dd865c49228784fa75b274246dab6");
    }

    @Test
    public void sha1AnonymizerTest() {
        Anonymizer anonymous = new Anonymizer("sha1");
        String hashCode = anonymous.generateHash("127.0.0.1", "fake user agent string");
        assertEquals(hashCode, "8595f3c66e0fa990128f7132a1e9d79bdf2dc44c");
    }

    @Test
    public void sha256AnonymizerTest() {
        Anonymizer anonymous = new Anonymizer("sha256");
        String hashCode = anonymous.generateHash("127.0.0.1", "fake user agent string");
        assertEquals(hashCode, "6fe0319198d60f5c09e58c945c289d4e08aaeb73165d9cb751184c8535cfc873");
    }

    @Test
    public void sha512AnonymizerTest() {
        Anonymizer anonymous = new Anonymizer("sha512");
        String hashCode = anonymous.generateHash("127.0.0.1", "fake user agent string");
        assertEquals(hashCode, "9cf08a87d37223d57f621610abff7b0f43b04afaafe447a0cc52b741c11ce557704c791c7e6bb9acba5a57671daa6872c3d13505adddbf3257c8d0525310e2d3");
    }


    @Test
    public void murmur3_32AnonymizerTest() {
        Anonymizer anonymous = new Anonymizer("murmur3_32");
        String hashCode = anonymous.generateHash("127.0.0.1", "fake user agent string");
        assertEquals(hashCode, "253ac1b8");
    }

    @Test
    public void murmur3_128AnonymizerTest() {
        Anonymizer anonymous = new Anonymizer("murmur3_32");
        String hashCode = anonymous.generateHash("127.0.0.1", "fake user agent string");
        assertEquals(hashCode, "253ac1b8");
    }

}
