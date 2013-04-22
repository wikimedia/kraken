package org.wikimedia.analytics.kraken.pig.maps;

import datafu.pig.util.SimpleEvalFunc;

import java.util.Map;

public class MapToKVPairs extends SimpleEvalFunc<String> {
    private final String itemSep;
    private final String kvSep;

    public MapToKVPairs() {
        this("&", "=");
    }

    public MapToKVPairs(String itemSep) {
        this(itemSep, "=");
    }

    public MapToKVPairs(String itemSep, String kvSep) {
        this.itemSep = itemSep;
        this.kvSep = kvSep;
    }

    public String call(final Map<String, String> map) {
        StringBuilder b = new StringBuilder();

        return b.toString();
    }

}
