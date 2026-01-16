package es.ulpgc.bd.indexing.service;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
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
    private final IMap<String, Map<Integer, Integer>> inverted;

    private final Pattern token = Pattern.compile("[\\p{L}\\p{N}]+");

    public IndexingService(HazelcastInstance hz) {
        this.hz = hz;
        this.lock = hz.getCPSubsystem().getLock("inverted-index-lock");
        this.docs = hz.getMap("docs");
        this.docTerms = hz.getMap("docTerms");
        this.inverted = hz.getMap("inverted");
    }

    public Map<String, Object> update(int bookId, String ingestionBaseUrl) throws Exception {
        if (ingestionBaseUrl == null || ingestionBaseUrl.isBlank()) {
            return Map.of("book_id", bookId, "status", "error", "message", "missing ingestion base url");
        }

        lock.lock();
        try {
            removeIfExists(bookId);

            String header = ingestion.fetchHeader(ingestionBaseUrl, bookId);
            String body = ingestion.fetchBody(ingestionBaseUrl, bookId);
            String meta = ingestion.fetchMetaOrNull(ingestionBaseUrl, bookId);

            Map<String, Object> metaObj = parseMeta(meta);
            Map<String, Object> doc = parseHeader(header, metaObj);
            Map<String, Integer> tf = termFreq(body);

            docs.put(bookId, doc);
            docTerms.put(bookId, tf);

            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                String term = e.getKey();
                int f = e.getValue();

                Map<Integer, Integer> postings = inverted.get(term);
                if (postings == null) postings = new HashMap<>();

                postings.put(bookId, f);
                inverted.put(term, postings);
            }

            return Map.of(
                    "book_id", bookId,
                    "status", "ok",
                    "ingestion", ingestionBaseUrl,
                    "terms", tf.size()
            );
        } finally {
            try { lock.unlock(); } catch (Exception ignored) {}
        }
    }

    private void removeIfExists(int bookId) {
        Map<String, Integer> oldTf = docTerms.get(bookId);
        if (oldTf != null) {
            for (String term : oldTf.keySet()) {
                Map<Integer, Integer> postings = inverted.get(term);
                if (postings != null) {
                    postings.remove(bookId);
                    if (postings.isEmpty()) inverted.remove(term);
                    else inverted.put(term, postings);
                }
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
                "terms", inverted.size()
        );
    }
}
