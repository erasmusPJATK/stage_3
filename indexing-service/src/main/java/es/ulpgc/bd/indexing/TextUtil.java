package es.ulpgc.bd.indexing;

import java.text.Normalizer;
import java.util.*;

public class TextUtil {
    private static final Set<String> STOP = new HashSet<>(Arrays.asList(
            "the","and","of","to","in","a","is","it","that","for","on","as","with","was","were","be","by","at","an","or","from","this","which","but","not","are","his","her","their","its","have","has","had","you","i","he","she","we","they","them","me","my","our","your"
    ));
    public static List<String> tokenize(String text) {
        if (text==null || text.isEmpty()) return Collections.emptyList();
        String t = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("\\p{M}+","");
        t = t.toLowerCase(Locale.ROOT).replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}\\s]"," ");
        String[] parts = t.split("\\s+");
        List<String> out = new ArrayList<>(parts.length);
        for(String p:parts){ if(p.isEmpty()) continue; if(STOP.contains(p)) continue; if(p.length()==1 && !Character.isDigit(p.charAt(0))) continue; out.add(p); }
        return out;
    }
    public static DocMeta parseHeader(String header, int id){
        DocMeta m = new DocMeta();
        m.bookId = id; m.title=""; m.author=""; m.language=""; m.year=0;
        if(header==null) return m;
        String[] lines = header.split("\\r?\\n");
        for(String ln: lines){
            int i = ln.indexOf(':');
            if(i<=0) continue;
            String k = ln.substring(0,i).trim().toLowerCase(Locale.ROOT);
            String v = ln.substring(i+1).trim();
            if(k.equals("title")) m.title=v;
            else if(k.equals("author")) m.author=v;
            else if(k.equals("language")) m.language=v;
            else if(k.equals("year")) { try{ m.year=Integer.parseInt(v.replaceAll("[^0-9]","")); }catch(Exception ignored){} }
        }
        return m;
    }
}
