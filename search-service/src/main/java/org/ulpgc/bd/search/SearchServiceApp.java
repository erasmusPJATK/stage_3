package org.ulpgc.bd.search;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import io.javalin.Javalin;
import io.javalin.json.JavalinGson;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class SearchServiceApp {

    public static void main(String[] args) {
        Args a = new Args(args);
        int port = a.getInt("port", 7003);
        String hzMembers = a.get("hz", "127.0.0.1:5701");
        String cluster = a.get("cluster", "bd-stage3");
        int hzPort = a.getInt("hzPort", 5701);

        HazelcastInstance hz = HazelcastBoot.start(cluster, hzMembers, hzPort);
        MultiMap<String, Integer> inverted = hz.getMultiMap("inverted-index");
        IMap<Integer, Object> docs = hz.getMap("docs");
        IMap<Integer, Map<String, Integer>> docTerms = hz.getMap("doc-terms");

        Javalin app = Javalin.create(cfg -> {
            cfg.http.defaultContentType = "application/json";
            cfg.jsonMapper(new JavalinGson());
        }).start(port);

        app.get("/hz/members", ctx -> {
            List<String> ms = hz.getCluster().getMembers().stream()
                    .map(Member::toString)
                    .collect(Collectors.toList());
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("members", ms);
            ctx.json(payload);
        });

        app.get("/hz/stats", ctx -> {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("docs", docs.size());
            payload.put("terms", inverted.keySet().size());
            ctx.json(payload);
        });

        app.get("/search", ctx -> {
            String q = Optional.ofNullable(ctx.queryParam("q"))
                    .orElse("")
                    .trim()
                    .toLowerCase(Locale.ROOT);
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            int k = 10;
            try {
                k = Integer.parseInt(Optional.ofNullable(ctx.queryParam("k")).orElse("10"));
            } catch (Exception ignored) {}

            List<String> terms = Arrays.stream(q.split("\\s+"))
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            Set<Integer> cand = new HashSet<>();
            for (String t : terms) {
                Collection<Integer> postings = inverted.get(t);
                if (postings != null) cand.addAll(postings);
            }

            int N = Math.max(docs.size(), 1);
            Map<Integer, Double> scores = new HashMap<>();

            for (String t : terms) {
                Collection<Integer> postings = inverted.get(t);
                if (postings == null || postings.isEmpty()) continue;
                int df = postings.size();
                double idf = Math.log(1.0 + (double) N / df);
                for (Integer docId : postings) {
                    Map<String, Integer> tfm = docTerms.get(docId);
                    int tf = tfm != null ? tfm.getOrDefault(t, 0) : 0;
                    if (tf <= 0) continue;
                    double s = tf * idf;
                    scores.merge(docId, s, Double::sum);
                }
            }

            List<Map<String, Object>> results = cand.stream()
                    .filter(id -> {
                        Object raw = docs.get(id);
                        if (raw == null) return false;
                        String aVal = getField(raw, "author");
                        String lVal = getField(raw, "language");
                        if (author != null && !author.equalsIgnoreCase(aVal)) return false;
                        if (language != null && !language.equalsIgnoreCase(lVal)) return false;
                        return true;
                    })
                    .sorted((a1, a2) -> Double.compare(
                            scores.getOrDefault(a2, 0.0),
                            scores.getOrDefault(a1, 0.0)
                    ))
                    .limit(k)
                    .map(id -> {
                        Object raw = docs.get(id);
                        String title = getField(raw, "title");
                        String aVal = getField(raw, "author");
                        String lVal = getField(raw, "language");

                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("book_id", id);
                        row.put("title", title);
                        row.put("author", aVal);
                        row.put("language", lVal);
                        row.put("score", scores.getOrDefault(id, 0.0));
                        return row;
                    })
                    .collect(Collectors.toList());

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("query", q);
            Map<String, Object> filters = new LinkedHashMap<>();
            if (author != null) filters.put("author", author);
            if (language != null) filters.put("language", language);
            payload.put("filters", filters);
            payload.put("count", results.size());
            payload.put("results", results);

            ctx.json(payload);
        });

        app.get("/status", ctx -> {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("service", "search");
            payload.put("port", port);
            payload.put("hzMembers", hzMembers);
            ctx.json(payload);
        });
    }

    private static String getField(Object raw, String field) {
        if (raw == null) return "";
        if (raw instanceof Map<?, ?> map) {
            Object v = map.get(field);
            return v != null ? v.toString() : "";
        }
        try {
            Method m = raw.getClass().getMethod("getString", String.class);
            Object v = m.invoke(raw, field);
            return v != null ? v.toString() : "";
        } catch (Exception ignored) {
        }
        return "";
    }
}
