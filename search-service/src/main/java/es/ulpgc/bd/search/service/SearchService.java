package es.ulpgc.bd.search.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.*;
import java.util.regex.Pattern;

public class SearchService {

    // ✅ MUSI pasować do IndexingService:
    private static final String MAP_DOCS = "docs";
    private static final String MAP_DOC_TERMS = "docTerms";
    private static final String MAP_INVERTED = "inverted";

    private static final Pattern TOKEN = Pattern.compile("[^\\p{IsAlphabetic}\\p{IsDigit}]+");

    private final HazelcastInstance hz;
    private final String hzCluster;
    private final String hzContact;
    private final int port;

    private final IMap<Integer, Map<String, Object>> docs;
    private final IMap<Integer, Map<String, Integer>> docTerms;
    private final IMap<String, Map<Integer, Integer>> inverted;

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
        out.put("docTermsDocs", docTerms.size());
        out.put("terms", inverted.size()); // liczba kluczy-termów

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

        // Bonus debug: jakie obiekty są realnie w klastrze
        List<String> distributed = hz.getDistributedObjects().stream()
                .map(o -> o.getServiceName() + " -> " + o.getName())
                .sorted()
                .toList();
        out.put("distributedObjects", distributed);

        return out;
    }

    public List<Map<String, Object>> search(String q, String author, String language, Integer year, int limit) {
        List<String> terms = tokenize(q);
        if (terms.isEmpty()) return List.of();

        // ✅ candidates = suma (OR) dokumentów z posting list dla każdego termu
        Set<Integer> candidates = new HashSet<>();
        for (String term : terms) {
            Map<Integer, Integer> posting = inverted.get(term);
            if (posting != null) {
                candidates.addAll(posting.keySet());
            }
        }

        if (candidates.isEmpty()) return List.of();

        int N = Math.max(1, docs.size());

        // trzymamy top-N, najmniejszy score wypada jako pierwszy
        PriorityQueue<Map<String, Object>> pq = new PriorityQueue<>(Comparator.comparingDouble(m -> (double) m.get("score")));

        for (Integer docId : candidates) {
            Map<String, Object> meta = docs.get(docId);
            if (meta == null) continue;
            if (!passesFilters(meta, author, language, year)) continue;

            Map<String, Integer> tfMap = docTerms.get(docId);
            if (tfMap == null) continue;

            double score = 0.0;

            for (String term : terms) {
                int tf = tfMap.getOrDefault(term, 0);
                if (tf <= 0) continue;

                Map<Integer, Integer> posting = inverted.get(term);
                int df = (posting == null) ? 0 : posting.size();

                // klasyczny smoothed idf
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
            int yi = toIntOrZero(y);
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

    private int toIntOrZero(Object o) {
        if (o == null) return 0;
        if (o instanceof Integer i) return i;
        if (o instanceof Long l) return (int) l.longValue();
        if (o instanceof Double d) return (int) d.doubleValue();
        if (o instanceof String s) {
            try { return Integer.parseInt(s.trim()); } catch (Exception ignored) {}
        }
        return 0;
    }

    private String str(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o);
        return s.isBlank() ? null : s;
    }
}
