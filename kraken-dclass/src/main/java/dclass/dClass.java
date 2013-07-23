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
package dclass;



import java.io.IOException;
import java.util.Map;
import java.util.HashMap;


public class dClass {
    private String path;
    
    /*
     * Note:
     *
     * the dclass_index property of this class is being set by 
     * the shared object native method setindex()
     *
     * which is being called from native method init()
     *
     */
    private long dclass_index;
    private boolean isDClassInitialized = false;


    private native int init(String file);
    private native void free(long index);
    
    private native long classify(long index,String s);

    private native int kvlength(long kv);
    private native String kvgetid(long kv);
    private native String kvgetkey(long kv,int pos);
    private native String kvgetvalue(long kv,int pos);


    private long getDclassIndex() {
        return dclass_index;
    };
    

    private static final String OS = System.getProperty("os.name").toLowerCase();

    public dClass(String s)
    {
        path=s;
        dclass_index=0;
        isDClassInitialized = ( init(s) >= 0);
    }

    public Map<String,String> classify(String s)
    {
        Map<String,String> ret=new HashMap<String,String>();

        
        
        if(isDClassInitialized && s!=null)
        {
            long kv;

            
            kv=classify(dclass_index,s);
            
            if(kv!=0)
            {
                int len=kvlength(kv);
                String id=kvgetid(kv);
                for(int i=0;i<len;i++)
                {
                    String key=kvgetkey(kv,i);
                    String value=kvgetvalue(kv,i);
                    ret.put(key,value);
                }
                ret.put("id",id);
            }
        }

        return ret;
    }


    
    protected void finalize() {
        if (isDClassInitialized) 
          free(this.getDclassIndex());
        isDClassInitialized = false;
        
    }

    public static void loadDclassSharedObject() throws IOException {
        try {
            if (OS.contains("mac")) {
                System.load("/usr/local/lib/libdclassjni.0.dylib");
            } else if (OS.contains("nix")
                    || OS.contains("nux")
                    || OS.contains("aix")) {
            
              try {
                System.loadLibrary("dclassjni");
              } catch(Exception e) {
                System.err.println("Error: " + e);
                System.exit(-1);
              };

            } else {
                System.err.println("OS not supported.");
                System.exit(-1);
            }
        } catch (UnsatisfiedLinkError e) {
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



}

