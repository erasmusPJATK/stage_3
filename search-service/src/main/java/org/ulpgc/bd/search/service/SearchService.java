package org.ulpgc.bd.search.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.ulpgc.bd.search.model.SearchResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Instant;
import java.util.*;

public class SearchService {

    private final Path datamarts;
    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();
    private static final Set<String> STOP_EN = new HashSet<>(Arrays.asList(
            "the","and","of","to","in","a","is","it","that","for","on","as","with","was","were","be","by","at","an",
            "or","from","this","which","but","not","are","his","her","their","its","have","has","had","you","i","he",
            "she","we","they","them","me","my","our","your"
    ));

    public SearchService(Path datamarts) {
        this.datamarts = datamarts;
    }

    public List<SearchResult> search(String q, String author, String language, Integer year, int k) {
        List<SearchResult> results = new ArrayList<>();
        try {
            String query = q == null ? "" : q;
            List<String> terms = tokenize(query);
            if (terms.isEmpty()) return results;

            int totalDocs = countDocs();
            if (totalDocs == 0) return results;

            Map<Integer, Double> scores = new HashMap<>();

            for (String term : terms) {
                String shard = shardName(term);
                Path shardFile = datamarts.resolve("inverted").resolve(shard + ".json");
                if (!Files.exists(shardFile)) continue;
                String json = Files.readString(shardFile, StandardCharsets.UTF_8);
                if (json.isBlank()) continue;
                JsonObject shardObj = G.fromJson(json, JsonObject.class);
                if (shardObj == null || !shardObj.has(term)) continue;
                JsonObject postings = shardObj.getAsJsonObject(term);
                if (postings == null || postings.size() == 0) continue;
                int df = postings.size();
                double idf = Math.log(1.0 + (double) totalDocs / (double) df);
                for (Map.Entry<String, JsonElement> e : postings.entrySet()) {
                    int docId;
                    try {
                        docId = Integer.parseInt(e.getKey());
                    } catch (NumberFormatException ex) {
                        continue;
                    }
                    int tf = e.getValue().getAsInt();
                    double contrib = tf * idf;
                    scores.merge(docId, contrib, Double::sum);
                }
            }

            if (scores.isEmpty()) return results;

            List<Map.Entry<Integer, Double>> ranked = new ArrayList<>(scores.entrySet());
            ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            int limit = k > 0 ? k : 10;
            for (Map.Entry<Integer, Double> e : ranked) {
                if (results.size() >= limit) break;
                int docId = e.getKey();
                Path docPath = datamarts.resolve("docs").resolve(docId + ".json");
                if (!Files.exists(docPath)) continue;
                String json = Files.readString(docPath, StandardCharsets.UTF_8);
                if (json.isBlank()) continue;
                JsonObject obj = G.fromJson(json, JsonObject.class);
                if (obj == null) continue;
                String title = optString(obj, "title");
                String docAuthor = optString(obj, "author");
                String lang = optString(obj, "language");
                int yr = optInt(obj, "year");
                if (author != null && !author.isBlank() && !author.equalsIgnoreCase(docAuthor)) continue;
                if (language != null && !language.isBlank() && !language.equalsIgnoreCase(lang)) continue;
                if (year != null && year > 0 && yr != year) continue;
                double score = e.getValue();
                results.add(new SearchResult(docId, title, docAuthor, lang, yr, score));
            }

            return results;
        } catch (Exception ex) {
            return results;
        }
    }

    public Map<String, Object> refresh() {
        return Map.of("refreshed", true, "ts", Instant.now().toString());
    }

    private int countDocs() throws IOException {
        Path docsDir = datamarts.resolve("docs");
        if (!Files.exists(docsDir)) return 0;
        try (var stream = Files.list(docsDir)) {
            int count = 0;
            for (Path p : (Iterable<Path>) stream::iterator) {
                if (p.getFileName() != null && p.getFileName().toString().endsWith(".json")) {
                    count++;
                }
            }
            return count;
        }
    }

    private List<String> tokenize(String text) {
        if (text == null || text.isBlank()) return Collections.emptyList();
        String t = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
        t = t.toLowerCase(Locale.ROOT).replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}\\s]", " ");
        String[] parts = t.split("\\s+");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            if (p.isEmpty()) continue;
            if (STOP_EN.contains(p)) continue;
            if (p.length() == 1 && !Character.isDigit(p.charAt(0))) continue;
            out.add(p);
        }
        return out;
    }

    private String shardName(String term) {
        if (term == null || term.isEmpty()) return "other";
        char c = term.charAt(0);
        if (c >= 'a' && c <= 'z') return String.valueOf(c);
        if (c >= '0' && c <= '9') return "num";
        return "other";
    }

    private String optString(JsonObject obj, String field) {
        if (obj.has(field) && !obj.get(field).isJsonNull()) {
            return obj.get(field).getAsString();
        }
        return "";
    }

    private int optInt(JsonObject obj, String field) {
        if (obj.has(field) && !obj.get(field).isJsonNull()) {
            try { return obj.get(field).getAsInt(); } catch (Exception ignored) {}
        }
        return 0;
    }
}
