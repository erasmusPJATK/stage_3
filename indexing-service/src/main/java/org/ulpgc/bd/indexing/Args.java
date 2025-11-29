package org.ulpgc.bd.indexing;

import java.util.HashMap;
import java.util.Map;

public class Args {
    private final Map<String,String> m = new HashMap<>();
    public Args(String[] a){ for(String s:a){ if(s.startsWith("--")){ int i=s.indexOf('='); if(i>2){ m.put(s.substring(2,i), s.substring(i+1)); } else { m.put(s.substring(2), "true"); }}}}
    public String get(String k,String def){ return m.getOrDefault(k,def); }
    public int getInt(String k,int def){ try{ return Integer.parseInt(m.get(k)); }catch(Exception e){ return def; } }
}
