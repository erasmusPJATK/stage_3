package org.ulpgc.bd.indexing.util;

import java.text.Normalizer;
import java.util.*;

public class TextUtil {
    private static final Set<String> STOP_EN = new HashSet<>(Arrays.asList(
            "the","and","of","to","in","a","is","it","that","for","on","as","with","was","were","be","by","at","an","or","from","this","which","but","not","are","his","her","their","its","have","has","had","you","i","he","she","we","they","them","me","my","our","your"
    ));

    public static List<String> tokenize(String text, String language) {
        if (text == null || text.isEmpty()) return Collections.emptyList();
        String t = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
        t = t.toLowerCase(Locale.ROOT).replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}\\s]", " ");
        String[] parts = t.split("\\s+");
        List<String> out = new ArrayList<>(parts.length);
        boolean en = language != null && language.toLowerCase(Locale.ROOT).startsWith("en");
        for (String p : parts) {
            if (p.isEmpty()) continue;
            if (en && STOP_EN.contains(p)) continue;
            if (p.length() == 1 && !Character.isDigit(p.charAt(0))) continue;
            out.add(p);
        }
        return out;
    }
}
