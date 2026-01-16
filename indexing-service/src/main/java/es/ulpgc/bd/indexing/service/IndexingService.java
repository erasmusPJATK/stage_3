package es.ulpgc.bd.indexing.service;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import es.ulpgc.bd.indexing.client.IngestionClient;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexingService {

    private final HazelcastInstance hz;
    private final FencedLock lock;

    private final IngestionClient ingestion = new IngestionClient();
    private final Gson gson = new Gson();

    private final IMap<Integer, Map<String, Object>> docs;
    private final IMap<Integer, Map<String, Integer>> docTerms;
    private final MultiMap<String, Integer> invertedIndex;

    private final Pattern token = Pattern.compile("[\\p{L}\\p{N}]+");

    public IndexingService(HazelcastInstance hz) {
        this.hz = hz;
        this.lock = hz.getCPSubsystem().getLock("inverted-index-lock");
        this.docs = hz.getMap("docs");
        this.docTerms = hz.getMap("docTerms");
        this.invertedIndex = hz.getMultiMap("inverted-index");
    }

    public Map<String, Object> update(int bookId, String ingestionBaseUrl) throws Exception {
        if (ingestionBaseUrl == null || ingestionBaseUrl.isBlank()) {
            return Map.of("book_id", bookId, "status", "error", "message", "missing ingestion base url");
        }
        return update(bookId, List.of(ingestionBaseUrl));
    }

    public Map<String, Object> update(int bookId, List<String> ingestionBases) throws Exception {
        List<String> bases = normalizeBases(ingestionBases);

        if (bases.isEmpty()) {
            return Map.of("book_id", bookId, "status", "error", "message", "no ingestion sources provided");
        }

        lock.lock();
        try {
            removeIfExists(bookId);

            String usedBase = null;
            String header = null;
            String body = null;
            String meta = null;
            Exception lastErr = null;

            for (String base : bases) {
                try {
                    header = ingestion.fetchHeader(base, bookId);
                    body = ingestion.fetchBody(base, bookId);
                    meta = ingestion.fetchMetaOrNull(base, bookId);
                    usedBase = base;
                    break;
                } catch (Exception ex) {
                    lastErr = ex;
                }
            }

            if (usedBase == null) {
                return Map.of(
                        "book_id", bookId,
                        "status", "error",
                        "message", "cannot fetch document from any source",
                        "sources", bases,
                        "lastError", lastErr == null ? "" : lastErr.getMessage()
                );
            }

            Map<String, Object> metaObj = parseMeta(meta);
            Map<String, Object> doc = parseHeader(header, metaObj);
            Map<String, Integer> tf = termFreq(body);

            docs.put(bookId, doc);
            docTerms.put(bookId, tf);

            for (String term : tf.keySet()) {
                invertedIndex.put(term, bookId);
            }

            return Map.of(
                    "book_id", bookId,
                    "status", "ok",
                    "ingestion", usedBase,
                    "terms", tf.size()
            );

        } finally {
            try { lock.unlock(); } catch (Exception ignored) {}
        }
    }

    public Map<String, Object> rebuild(String ingestionBaseUrl) throws Exception {
        if (ingestionBaseUrl == null || ingestionBaseUrl.isBlank()) {
            return Map.of("status", "error", "message", "missing ingestion base url");
        }

        long t0 = System.nanoTime();

        List<Integer> ids = ingestion.listBooks(ingestionBaseUrl);
        int ok = 0;
        List<Integer> failed = new ArrayList<>();

        for (int id : ids) {
            try {
                Map<String, Object> out = update(id, ingestionBaseUrl);
                if ("ok".equalsIgnoreCase(String.valueOf(out.get("status")))) ok++;
                else failed.add(id);
            } catch (Exception ex) {
                failed.add(id);
            }
        }

        long ms = (System.nanoTime() - t0) / 1_000_000L;

        Map<String, Object> res = new LinkedHashMap<>();
        res.put("status", "ok");
        res.put("ingestion", ingestionBaseUrl);
        res.put("total", ids.size());
        res.put("indexed", ok);
        res.put("failed", failed.size());
        res.put("failedIds", failed);
        res.put("time_ms", ms);
        return res;
    }

    private void removeIfExists(int bookId) {
        Map<String, Integer> oldTf = docTerms.get(bookId);
        if (oldTf != null) {
            for (String term : oldTf.keySet()) {
                invertedIndex.remove(term, bookId);
            }
        }
        docTerms.remove(bookId);
        docs.remove(bookId);
    }

    private Map<String, Object> parseHeader(String header, Map<String, Object> metaObj) {
        String title = null;
        String author = null;
        String language = null;
        Integer year = null;

        if (header != null) {
            for (String line : header.split("\\R")) {
                String s = line.trim();
                String low = s.toLowerCase();
                if (low.startsWith("title:")) title = s.substring(6).trim();
                else if (low.startsWith("author:")) author = s.substring(7).trim();
                else if (low.startsWith("language:")) language = s.substring(9).trim();
                else if (low.startsWith("year:")) {
                    try { year = Integer.parseInt(s.substring(5).trim()); } catch (Exception ignored) {}
                }
            }
        }

        if (year == null && metaObj != null && metaObj.get("year") instanceof Number) {
            year = ((Number) metaObj.get("year")).intValue();
        }

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("title", title != null ? title : "Unknown");
        doc.put("author", author != null ? author : "Unknown");
        doc.put("language", language != null ? language : "Unknown");
        doc.put("year", year != null ? year : 0);
        return doc;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMeta(String metaJson) {
        if (metaJson == null || metaJson.isBlank()) return null;
        try {
            Object o = gson.fromJson(metaJson, Object.class);
            if (o instanceof Map) return (Map<String, Object>) o;
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private Map<String, Integer> termFreq(String body) {
        Map<String, Integer> tf = new HashMap<>();
        if (body == null) return tf;
        Matcher m = token.matcher(body.toLowerCase());
        while (m.find()) {
            String t = m.group();
            tf.put(t, tf.getOrDefault(t, 0) + 1);
        }
        return tf;
    }

    public Map<String, Object> stats() {
        return Map.of(
                "docs", docs.size(),
                "terms", invertedIndex.keySet().size()
        );
    }

    private static List<String> normalizeBases(List<String> in) {
        if (in == null) return List.of();
        LinkedHashSet<String> s = new LinkedHashSet<>();
        for (String x : in) {
            if (x == null) continue;
            String t = x.trim();
            if (t.isEmpty()) continue;
            if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
            s.add(t);
        }
        return new ArrayList<>(s);
    }
}
