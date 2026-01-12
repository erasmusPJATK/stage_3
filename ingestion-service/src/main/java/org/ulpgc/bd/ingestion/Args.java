package org.ulpgc.bd.ingestion;

import java.util.LinkedHashMap;
import java.util.Map;

public class Args {
    public static Map<String, String> parse(String[] args) {
        Map<String, String> out = new LinkedHashMap<>();
        if (args == null) return out;
        for (String a : args) {
            if (a == null) continue;
            String s = a.trim();
            if (!s.startsWith("--")) continue;
            s = s.substring(2);
            int eq = s.indexOf('=');
            if (eq < 0) out.put(s, "true");
            else out.put(s.substring(0, eq).trim(), s.substring(eq + 1).trim());
        }
        return out;
    }
}
