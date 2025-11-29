package org.ulpgc.bd.search;

import com.google.gson.Gson;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import io.javalin.Javalin;

import java.util.*;
import java.util.stream.Collectors;

public class SearchServiceApp {
    public static class DocMeta {
        public int bookId;
        public String title;
        public String author;
        public String language;
        public int year;
    }

    public static void main(String[] args) {
        Args a = new Args(args);
        int port = a.getInt("port",7003);
        String hzMembers = a.get("hz","127.0.0.1:5701");
        String cluster = a.get("cluster","bd-stage3");
        int hzPort = a.getInt("hzPort",5701);

        HazelcastInstance hz = HazelcastBoot.start(cluster, hzMembers, hzPort);
        MultiMap<String,Integer> inverted = hz.getMultiMap("inverted-index");
        IMap<Integer, DocMeta> docs = hz.getMap("docs");
        IMap<Integer, Map<String,Integer>> docTerms = hz.getMap("doc-terms");
        Gson G = new Gson();

        Javalin app = Javalin.create(c -> c.http.defaultContentType="application/json").start(port);

        app.get("/hz/members", ctx -> {
            List<String> ms = hz.getCluster().getMembers().stream().map(Member::toString).collect(Collectors.toList());
            ctx.json(Map.of("members", ms));
        });

        app.get("/hz/stats", ctx -> {
            ctx.json(Map.of(
                    "docs", docs.size(),
                    "terms", inverted.keySet().size()
            ));
        });

        app.get("/search", ctx -> {
            String q = Optional.ofNullable(ctx.queryParam("q")).orElse("").trim().toLowerCase(Locale.ROOT);
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String yearStr = ctx.queryParam("year");
            int k = 10;
            try { k = Integer.parseInt(Optional.ofNullable(ctx.queryParam("k")).orElse("10")); } catch(Exception ignored){}

            List<String> terms = Arrays.stream(q.split("\\s+")).filter(s->!s.isBlank()).collect(Collectors.toList());
            Set<Integer> cand = new HashSet<>();
            for(String t: terms) cand.addAll(inverted.get(t));
            int N = Math.max(docs.size(), 1);
            Map<Integer, Double> score = new HashMap<>();
            for(String t: terms){
                int df = Math.max(inverted.get(t).size(), 1);
                double idf = Math.log(1.0 + (double)N/df);
                for(Integer d : inverted.get(t)){
                    Map<String,Integer> tfm = docTerms.get(d);
                    int tf = tfm!=null ? tfm.getOrDefault(t,0) : 0;
                    double s = tf * idf;
                    score.merge(d, s, Double::sum);
                }
            }

            List<Map<String,Object>> res = cand.stream()
                    .filter(id -> {
                        DocMeta m = docs.get(id);
                        if(m==null) return false;
                        if(author!=null && !author.equalsIgnoreCase(m.author)) return false;
                        if(language!=null && !language.equalsIgnoreCase(m.language)) return false;
                        if(yearStr!=null) try{ if(Integer.parseInt(yearStr)!=m.year) return false; }catch(Exception ignored){}
                        return true;
                    })
                    .sorted((a1,a2)-> Double.compare(score.getOrDefault(a2,0.0), score.getOrDefault(a1,0.0)))
                    .limit(k)
                    .map(id -> {
                        DocMeta m = docs.get(id);
                        Map<String,Object> row = new LinkedHashMap<>();
                        row.put("book_id", id);
                        row.put("title", m==null?"":m.title);
                        row.put("author", m==null?"":m.author);
                        row.put("language", m==null?"":m.language);
                        row.put("year", m==null?0:m.year);
                        row.put("score", score.getOrDefault(id,0.0));
                        return row;
                    })
                    .collect(Collectors.toList());

            Map<String,Object> payload = new LinkedHashMap<>();
            payload.put("query", q);
            Map<String,Object> filters = new LinkedHashMap<>();
            if(author!=null) filters.put("author", author);
            if(language!=null) filters.put("language", language);
            if(yearStr!=null) filters.put("year", yearStr);
            payload.put("filters", filters);
            payload.put("count", res.size());
            payload.put("results", res);

            ctx.result(G.toJson(payload));
        });

        app.get("/status", ctx -> ctx.json(Map.of("service","search","port",port,"hzMembers",hzMembers)));
    }
}
