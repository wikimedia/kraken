package org.wikimedia.analytics.kraken.pig.maps;

import datafu.pig.util.SimpleEvalFunc;
import org.wikimedia.analytics.kraken.utils.KVUtils;

import java.util.Map;


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
        return KVUtils.kvToMap(kvPairs, itemSep, kvSep);
    }

}
