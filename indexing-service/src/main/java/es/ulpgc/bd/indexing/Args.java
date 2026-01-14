package es.ulpgc.bd.indexing;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Args {
    private Args() {}

    public static Map<String, String> parse(String[] args) {
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) continue;
            a = a.substring(2);
            String key;
            String val;
            int eq = a.indexOf('=');
            if (eq >= 0) {
                key = a.substring(0, eq).trim();
                val = a.substring(eq + 1).trim();
            } else {
                key = a.trim();
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    val = args[++i].trim();
                } else {
                    val = "true";
                }
            }
            if (!key.isEmpty()) out.put(key, val);
        }
        return out;
    }

    public static String get(Map<String, String> a, String... keys) {
        for (String k : keys) {
            String v = a.get(k);
            if (v != null) return v;
        }
        return null;
    }
}
