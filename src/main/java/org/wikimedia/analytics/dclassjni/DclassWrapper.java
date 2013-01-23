/**
 * Copyright (C) 2012-2013  Wikimedia Foundation

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
package org.wikimedia.analytics.dclassjni;


import java.io.IOException;
import java.util.Map;

public class DclassWrapper {
    /**
     *  allocate and load up dtrees in memory.
     */
    public native void initUA();

    /**
     *  deallocate dtrees(sort-of a destructor).
     */
    public native void destroyUA();

    /**
     * the good stuff (what we actually want to do, classify UAs).
     *
     * @param ua the useragent device strings
     * @return the @Map<String, String>
     */
    public native Map<String, String> classifyUA(String ua);

    /**
     * Pointer to dclass_index structure.
     */
    public long pointer_di;

    /**
     * Set_pointer_di.
     *
     * @param i the i
     * @return the long
     */
    public long set_pointer_di(long i) {
        pointer_di = i;
        return pointer_di;
    }

    /**
     * Gets the _pointer_di.
     *
     * @return the _pointer_di
     */
    public long get_pointer_di() {
        return pointer_di;
    }

    static String UserAgentSample = "Mozilla/5.0 (Linux; U; Android 2.2; en; HTC Aria A6380 Build/ERE27) AppleWebKit/540.13+ (KHTML, like Gecko) Version/3.1 Mobile Safari/524.15.0";
    static private final String os = System.getProperty("os.name").toLowerCase();

    static {
        try {
            loadDclassSharedObject();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        };
    };

    public static void main(String[] args){

//        // load libdclasswrapper.so
        Result result = new Result();
        result.classifyUseragent(UserAgentSample);
    }


    public static void loadDclassSharedObject() throws IOException {
        System.out.print("loadDclassSharedObject()\n");
        try {
            if (os.contains("mac")) {
                System.load("/usr/local/lib/libdclass.dylib");
                System.load("/usr/local/lib/libdclassjni.0.dylib");

            } else if (os.contains("nix") ||
                    os.contains("nux") ||
                    os.contains("aix")) {
                //System.load("/usr/lib/libdclass.so");
                System.load("/usr/lib/libdclassjni.so");
            } else {
                System.out.println("OS not supported.");
                System.exit(-1);
            }
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
}
