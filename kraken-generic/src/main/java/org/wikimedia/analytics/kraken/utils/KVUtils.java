package org.wikimedia.analytics.kraken.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class KVUtils {
    static public final String DEFAULT_ITEM_SEP = "&";
    static public final String DEFAULT_KV_SEP = "=";

    // Static utility class
    private KVUtils() {}


    static public Map<String, String> kvToMap(final String kvPairs) {
        return kvToMap(kvPairs, DEFAULT_ITEM_SEP, DEFAULT_KV_SEP);
    }

    static public Map<String, String> kvToMap(final String kvPairs, final String itemSep) {
        return kvToMap(kvPairs, itemSep, DEFAULT_KV_SEP);
    }

    static public Map<String, String> kvToMap(final String kvPairs, final String itemSep, final String kvSep) {
        checkNotNull(itemSep); checkNotNull(kvSep);
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


    static public String mapToKV(final Map<String, String> map) {
        return mapToKV(map, DEFAULT_ITEM_SEP, DEFAULT_KV_SEP);
    }

    static public String mapToKV(final Map<String, String> map, final String itemSep) {
        return mapToKV(map, itemSep, DEFAULT_KV_SEP);
    }

    static public String mapToKV(final Map<String, String> map, final String itemSep, final String kvSep) {
        checkNotNull(itemSep); checkNotNull(kvSep);
        if (map == null) return null;
        boolean isFirst = true;
        StringBuilder b = new StringBuilder();
        for (String key : map.keySet()) {
            String value = map.get(key);
            if (!isFirst) b.append(itemSep);
            try {
                b.append(URLEncoder.encode(key, "utf8"));
            } catch (UnsupportedEncodingException ex) {}
            b.append(kvSep); // always add sep, even when value is null
            if (value != null ) {
                try {
                    b.append(URLEncoder.encode(value, "utf8"));
                } catch (UnsupportedEncodingException ex) {}
            }
            isFirst = false;
        }
        return b.toString();
    }

}
