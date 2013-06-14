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


import java.io.File;
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
    public final long get_pointer_di() {
        return pointer_di;
    }


    private static final String OS = System.getProperty("os.name").toLowerCase();

    public static void loadDclassSharedObject() throws IOException {
        try {
            if (OS.contains("mac")) {
                System.load("/usr/local/lib/libdclassjni.0.dylib");
            } else if (OS.contains("nix")
                    || OS.contains("nux")
                    || OS.contains("aix")) {
                System.load("/usr/lib/libdclassjni.so");
            } else {
                System.err.println("OS not supported.");
                System.exit(-1);
            }
        } catch (UnsatisfiedLinkError e) {
            File f = new File("/usr/lib/libdclassjni.so");
            System.err.println("/usr/lib/libdclassjni.so exists: " + f.exists());
            System.err.println("/usr/lib/libdclassjni.so is readable: " + f.canRead());
            System.err.println("Native code library failed to load.\n" + e);
            e.printStackTrace();
        }
    }

    static {
        try {
            loadDclassSharedObject();
        } catch (IOException e) {
            System.err.println("Error loading dClass Shared Object: "+e.getMessage());
            e.printStackTrace();
        }
    }


    static String userAgentSample = "Mozilla/5.0 (Linux; U; Android 2.2; en; HTC Aria A6380 Build/ERE27) AppleWebKit/540.13+ (KHTML, like Gecko) Version/3.1 Mobile Safari/524.15.0";
    public static void main(final String[] args){
        DeviceClassification deviceClassification = new DeviceClassification();
        deviceClassification.classifyUseragent(userAgentSample);
    }


}

