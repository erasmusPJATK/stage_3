package es.ulpgc.bd.search;

import java.util.LinkedHashMap;
import java.util.Map;

public class Args {
    public static Map<String, String> parse(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        if (args == null) return m;
        for (String a : args) {
            if (a == null) continue;
            if (!a.startsWith("--")) continue;
            int i = a.indexOf('=');
            if (i > 2) m.put(a.substring(2, i).trim(), a.substring(i + 1).trim());
            else m.put(a.substring(2).trim(), "true");
        }
        return m;
    }
}
