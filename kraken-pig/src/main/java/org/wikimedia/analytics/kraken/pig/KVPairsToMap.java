package org.wikimedia.analytics.kraken.pig;

import datafu.pig.util.SimpleEvalFunc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;


public class KVPairsToMap extends SimpleEvalFunc<Map<String, String>> {
    private final String itemSep;
    private final String kvSep;
    
    public KVPairsToMap() {
        this("&", "=");
    }
    
    public KVPairsToMap(String itemSep) {
        this(itemSep, "=");
    }
    
    public KVPairsToMap(String itemSep, String kvSep) {
        this.itemSep = itemSep;
        this.kvSep = kvSep;
    }
    
    public Map<String, String> call(final String kvPairs){
        if (kvPairs == null) return null;
        Map<String, String> map = new HashMap<String, String>();
        for (String kv : kvPairs.split(itemSep)) {
            String[] pair = kv.split(kvSep, 2);
            if (pair.length == 0) continue;
            
            String key = pair[0].trim();
            try {
                key = URLDecoder.decode(key, "utf8");
            } catch (UnsupportedEncodingException e) {}
            
            String value = null;
            if (pair.length > 1) {
                value = pair[1].trim();
                try {
                    value = URLDecoder.decode(value, "utf8");
                } catch (UnsupportedEncodingException e) {}
            }
            
            map.put(key, value);
        }
        return map;
    }

}
