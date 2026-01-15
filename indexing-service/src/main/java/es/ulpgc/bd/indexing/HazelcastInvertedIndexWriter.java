package es.ulpgc.bd.indexing;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class HazelcastInvertedIndexWriter {

    public static final String MAP_DOCS = "docs";
    public static final String MAP_DOC_TERMS = "docTerms";
    public static final String MULTIMAP_INVERTED = "inverted-index";

    private HazelcastInvertedIndexWriter() {}

    public static IndexStats indexDocument(
            HazelcastInstance hz,
            int docId,
            String text,
            Map<String, Object> meta
    ) {
        if (hz == null) throw new IllegalArgumentException("HazelcastInstance is null");
        if (text == null) text = "";

        Map<String, Integer> tf = termFrequencies(text);

        IMap<Integer, Map<String, Object>> docs = hz.getMap(MAP_DOCS);
        docs.put(docId, meta == null ? Map.of("id", docId) : meta);

        IMap<Integer, Map<String, Integer>> docTerms = hz.getMap(MAP_DOC_TERMS);
        docTerms.put(docId, tf);

        MultiMap<String, Integer> invertedIndex = hz.getMultiMap(MULTIMAP_INVERTED);

        Set<String> uniqueTerms = tf.keySet();
        for (String term : uniqueTerms) {
            invertedIndex.put(term, docId);
        }

        return new IndexStats(docId, tf.size(), uniqueTerms.size());
    }

    private static Map<String, Integer> termFrequencies(String text) {
        Map<String, Integer> tf = new HashMap<>();
        if (text == null || text.isBlank()) return tf;

        String[] tokens = text
                .toLowerCase()
                .split("[^a-z0-9]+");

        for (String t : tokens) {
            if (t == null || t.isBlank()) continue;
            if (t.length() < 2) continue; // wycinamy syf typu "a", "i"

            tf.merge(t, 1, Integer::sum);
        }

        return tf;
    }

    public record IndexStats(int docId, int distinctTermsTf, int uniqueTermsIndexed) {}
}
