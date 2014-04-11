/**
 * Copyright (C) 2012-2013  Wikimedia Foundation

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

