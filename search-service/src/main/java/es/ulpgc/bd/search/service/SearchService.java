package es.ulpgc.bd.search.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.*;
import java.util.regex.Pattern;

public class SearchService {
    private static final String MAP_DOCS = "docs";
    private static final String MAP_DOC_TERMS = "doc-terms";
    private static final String MAP_INVERTED = "inverted-index";

    private static final Pattern TOKEN = Pattern.compile("[^\\p{IsAlphabetic}\\p{IsDigit}]+");

    private final HazelcastInstance hz;
    private final String hzCluster;
    private final String hzContact;
    private final int port;

    private final IMap<Integer, Map<String, Object>> docs;
    private final IMap<Integer, Map<String, Object>> docTerms;
    private final IMap<String, Object> inverted;

    public SearchService(HazelcastInstance hz, String hzCluster, String hzContact, int port) {
        this.hz = hz;
        this.hzCluster = hzCluster;
        this.hzContact = hzContact;
        this.port = port;

        this.docs = hz.getMap(MAP_DOCS);
        this.docTerms = hz.getMap(MAP_DOC_TERMS);
        this.inverted = hz.getMap(MAP_INVERTED);
    }

    public Map<String, Object> status() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("service", "search");
        out.put("port", port);
        out.put("hzCluster", hzCluster);
        out.put("hz", hzContact);
        out.put("docs", docs.size());
        out.put("terms", inverted.size());
        return out;
    }

    public Map<String, Object> hzStats() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cluster", hz.getConfig().getClusterName());

        List<String> members = hz.getCluster().getMembers().stream()
                .map(m -> m.getAddress().toString())
                .toList();
        out.put("members", members);

        out.put("maps", Map.of(
                MAP_DOCS, docs.size(),
                MAP_DOC_TERMS, docTerms.size(),
                MAP_INVERTED, inverted.size()
        ));
        return out;
    }

    public List<Map<String, Object>> search(String q, String author, String language, Integer year, int limit) {
        List<String> terms = tokenize(q);
        if (terms.isEmpty()) return List.of();

        Set<Integer> candidates = new HashSet<>();
        for (String term : terms) {
            candidates.addAll(postingToDocIds(inverted.get(term)));
        }

        if (candidates.isEmpty()) return List.of();

        int N = Math.max(1, docs.size());
        PriorityQueue<Map<String, Object>> pq = new PriorityQueue<>(Comparator.comparingDouble(m -> (double) m.get("score")));

        for (Integer docId : candidates) {
            Map<String, Object> meta = docs.get(docId);
            if (meta == null) continue;
            if (!passesFilters(meta, author, language, year)) continue;

            Map<String, Object> tfMap = docTerms.get(docId);
            if (tfMap == null) continue;

            double score = 0.0;
            for (String term : terms) {
                int tf = getInt(tfMap.get(term));
                if (tf <= 0) continue;

                int df = postingDf(inverted.get(term));
                double idf = Math.log((N + 1.0) / (df + 1.0)) + 1.0;
                score += tf * idf;
            }

            if (score <= 0) continue;

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("book_id", docId);
            row.put("title", str(meta.get("title")));
            row.put("author", str(meta.get("author")));
            row.put("language", str(meta.get("language")));
            row.put("year", meta.get("year"));
            row.put("score", score);

            pq.offer(row);
            if (pq.size() > limit) pq.poll();
        }

        List<Map<String, Object>> out = new ArrayList<>();
        while (!pq.isEmpty()) out.add(pq.poll());
        Collections.reverse(out);
        return out;
    }

    private boolean passesFilters(Map<String, Object> meta, String author, String language, Integer year) {
        if (author != null && !author.isBlank()) {
            String a = str(meta.get("author"));
            if (a == null || !a.toLowerCase().contains(author.toLowerCase())) return false;
        }
        if (language != null && !language.isBlank()) {
            String l = str(meta.get("language"));
            if (l == null || !l.equalsIgnoreCase(language)) return false;
        }
        if (year != null) {
            Object y = meta.get("year");
            if (y == null) return false;
            int yi = getInt(y);
            if (yi != year) return false;
        }
        return true;
    }

    private List<String> tokenize(String q) {
        if (q == null) return List.of();
        String[] parts = TOKEN.split(q.toLowerCase().trim());
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            if (!p.isBlank()) out.add(p);
        }
        return out;
    }

    private Set<Integer> postingToDocIds(Object posting) {
        if (posting == null) return Set.of();

        Set<Integer> out = new HashSet<>();

        if (posting instanceof Collection<?> c) {
            for (Object o : c) {
                Integer id = toIntOrNull(o);
                if (id != null) out.add(id);
            }
            return out;
        }

        if (posting instanceof Map<?, ?> m) {
            for (Object k : m.keySet()) {
                Integer id = toIntOrNull(k);
                if (id != null) out.add(id);
            }
            return out;
        }

        Integer single = toIntOrNull(posting);
        if (single != null) out.add(single);

        return out;
    }

    private int postingDf(Object posting) {
        if (posting == null) return 0;
        if (posting instanceof Collection<?> c) return c.size();
        if (posting instanceof Map<?, ?> m) return m.size();
        return 1;
    }

    private Integer toIntOrNull(Object o) {
        if (o == null) return null;
        if (o instanceof Integer i) return i;
        if (o instanceof Long l) return (int) l.longValue();
        if (o instanceof Double d) return (int) d.doubleValue();
        if (o instanceof String s) {
            try { return Integer.parseInt(s.trim()); } catch (Exception ignored) {}
        }
        return null;
    }

    private int getInt(Object o) {
        Integer v = toIntOrNull(o);
        return v == null ? 0 : v;
    }

    private String str(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o);
        return s.isBlank() ? null : s;
    }
}
