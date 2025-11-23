package org.ulpgc.bd.indexing.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.ulpgc.bd.indexing.model.DocMeta;
import org.ulpgc.bd.indexing.model.IndexStatus;
import org.ulpgc.bd.indexing.util.TextUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexingService {
    private final Path datalake;
    private final Path datamarts;
    private final String indexerVersion;
    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();

    public IndexingService(Path datalake, Path datamarts, String indexerVersion) {
        this.datalake = datalake;
        this.datamarts = datamarts;
        this.indexerVersion = indexerVersion;
    }

    public Map<String, Object> updateOne(int bookId) {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            var pair = findLatestHeaderBody(bookId);
            if (pair == null) throw new IOException("book not found in datalake");
            Path header = pair[0];
            Path body = pair[1];

            DocMeta meta = readMeta(header, bookId);
            String text = Files.readString(body, StandardCharsets.UTF_8);
            Map<String, Integer> tf = countTerms(text, meta.language);

            Path docsDir = datamarts.resolve("docs");
            Files.createDirectories(docsDir);
            Path docPath = docsDir.resolve(bookId + ".json");
            writeAtomic(docPath, G.toJson(Map.of(
                    "book_id", bookId,
                    "title", meta.title,
                    "author", meta.author,
                    "language", meta.language,
                    "year", meta.year,
                    "terms", tf
            )));

            updateInverted(tf, bookId);

            updateStatus();

            out.put("book_id", bookId);
            out.put("index", "updated");
            out.put("doc_terms", tf.size());
            out.put("title", meta.title);
            out.put("author", meta.author);
            out.put("language", meta.language);
            out.put("year", meta.year);
        } catch (Exception e) {
            out.clear();
            out.put("book_id", bookId);
            out.put("status", "error");
            out.put("message", e.getMessage());
        }
        return out;
    }

    public Map<String, Object> rebuildAll() {
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            Map<Integer, Path[]> files = listAllBooks();
            Path docsDir = datamarts.resolve("docs");
            Path invDir = datamarts.resolve("inverted");
            if (Files.exists(invDir)) deleteRecursively(invDir);
            Files.createDirectories(invDir);
            if (!Files.exists(docsDir)) Files.createDirectories(docsDir);

            Map<String, Map<Integer, Integer>> inverted = new HashMap<>();
            int processed = 0;

            for (Map.Entry<Integer, Path[]> e : files.entrySet()) {
                int bookId = e.getKey();
                Path header = e.getValue()[0];
                Path body = e.getValue()[1];

                DocMeta meta = readMeta(header, bookId);
                String text = Files.readString(body, StandardCharsets.UTF_8);
                Map<String, Integer> tf = countTerms(text, meta.language);

                Path docPath = docsDir.resolve(bookId + ".json");
                writeAtomic(docPath, G.toJson(Map.of(
                        "book_id", bookId,
                        "title", meta.title,
                        "author", meta.author,
                        "language", meta.language,
                        "year", meta.year,
                        "terms", tf
                )));

                for (Map.Entry<String, Integer> t : tf.entrySet()) {
                    inverted.computeIfAbsent(t.getKey(), k -> new HashMap<>()).put(bookId, t.getValue());
                }
                processed++;
            }

            writeInvertedShards(inverted);

            updateStatus();

            out.put("books_processed", processed);
            out.put("elapsed_time", "");
        } catch (Exception e) {
            out.clear();
            out.put("status", "error");
            out.put("message", e.getMessage());
        }
        return out;
    }

    public Map<String, Object> status() {
        try {
            IndexStatus st = computeStatus();
            return Map.of(
                    "books_indexed", st.books_indexed,
                    "last_update", st.last_update,
                    "index_size_MB", st.index_size_MB,
                    "indexer_version", st.indexer_version
            );
        } catch (Exception e) {
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    private Map<String, Integer> countTerms(String body, String language) {
        List<String> toks = TextUtil.tokenize(body, language);
        Map<String, Integer> tf = new HashMap<>();
        for (String t : toks) tf.merge(t, 1, Integer::sum);
        return tf;
    }

    private void updateInverted(Map<String, Integer> tf, int bookId) throws IOException {
        Path invDir = datamarts.resolve("inverted");
        Files.createDirectories(invDir);
        Map<String, Map<Integer, Integer>> byShard = new HashMap<>();
        for (Map.Entry<String, Integer> e : tf.entrySet()) {
            String term = e.getKey();
            String shard = shardName(term);
            byShard.computeIfAbsent(shard, k -> new HashMap<>());
        }
        for (String shard : byShard.keySet()) {
            Path shardFile = invDir.resolve(shard + ".json");
            Map<String, Map<String, Integer>> data = new HashMap<>();
            if (Files.exists(shardFile)) {
                String s = Files.readString(shardFile, StandardCharsets.UTF_8);
                if (!s.isBlank()) data = G.fromJson(s, data.getClass());
            }
            if (data == null) data = new HashMap<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                String term = e.getKey();
                if (!shard.equals(shardName(term))) continue;
                Map<String, Integer> postings = data.computeIfAbsent(term, k -> new HashMap<>());
                postings.put(String.valueOf(bookId), e.getValue());
            }
            writeAtomic(shardFile, G.toJson(data));
        }
    }

    private void writeInvertedShards(Map<String, Map<Integer, Integer>> inverted) throws IOException {
        Path invDir = datamarts.resolve("inverted");
        Files.createDirectories(invDir);
        Map<String, Map<String, Map<String, Integer>>> shards = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Integer>> e : inverted.entrySet()) {
            String term = e.getKey();
            String shard = shardName(term);
            Map<String, Map<String, Integer>> shardMap = shards.computeIfAbsent(shard, k -> new HashMap<>());
            Map<String, Integer> postings = new HashMap<>();
            for (Map.Entry<Integer, Integer> p : e.getValue().entrySet()) {
                postings.put(String.valueOf(p.getKey()), p.getValue());
            }
            shardMap.put(term, postings);
        }
        for (Map.Entry<String, Map<String, Map<String, Integer>>> s : shards.entrySet()) {
            Path shardFile = invDir.resolve(s.getKey() + ".json");
            writeAtomic(shardFile, G.toJson(s.getValue()));
        }
    }

    private String shardName(String term) {
        if (term == null || term.isEmpty()) return "other";
        char c = term.charAt(0);
        if (c >= 'a' && c <= 'z') return String.valueOf(c);
        if (c >= '0' && c <= '9') return "num";
        return "other";
    }

    private Path[] findLatestHeaderBody(int bookId) throws IOException {
        if (!Files.exists(datalake)) return null;
        Pattern pHeader = Pattern.compile("^(\\d+)_header\\.txt$");
        Pattern pBody = Pattern.compile("^(\\d+)_body\\.txt$");
        List<Path> headers = new ArrayList<>();
        List<Path> bodies = new ArrayList<>();
        try (var stream = Files.walk(datalake)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                Matcher mh = pHeader.matcher(fn);
                Matcher mb = pBody.matcher(fn);
                if (mh.matches() && Integer.parseInt(mh.group(1)) == bookId) headers.add(p);
                if (mb.matches() && Integer.parseInt(mb.group(1)) == bookId) bodies.add(p);
            }
        }
        if (headers.isEmpty() || bodies.isEmpty()) return null;
        headers.sort(this::compareDatalakePath);
        bodies.sort(this::compareDatalakePath);
        Path h = headers.get(headers.size() - 1);
        Path b = bodies.get(bodies.size() - 1);
        return new Path[]{h, b};
    }

    private Map<Integer, Path[]> listAllBooks() throws IOException {
        Map<Integer, Path[]> map = new HashMap<>();
        if (!Files.exists(datalake)) return map;
        Pattern pHeader = Pattern.compile("^(\\d+)_header\\.txt$");
        Pattern pBody = Pattern.compile("^(\\d+)_body\\.txt$");
        Map<Integer, List<Path>> hs = new HashMap<>();
        Map<Integer, List<Path>> bs = new HashMap<>();
        try (var stream = Files.walk(datalake)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                Matcher mh = pHeader.matcher(fn);
                Matcher mb = pBody.matcher(fn);
                if (mh.matches()) {
                    int id = Integer.parseInt(mh.group(1));
                    hs.computeIfAbsent(id, k -> new ArrayList<>()).add(p);
                } else if (mb.matches()) {
                    int id = Integer.parseInt(mb.group(1));
                    bs.computeIfAbsent(id, k -> new ArrayList<>()).add(p);
                }
            }
        }
        for (Map.Entry<Integer, List<Path>> e : hs.entrySet()) {
            int id = e.getKey();
            if (!bs.containsKey(id)) continue;
            List<Path> hlist = e.getValue();
            List<Path> blist = bs.get(id);
            hlist.sort(this::compareDatalakePath);
            blist.sort(this::compareDatalakePath);
            map.put(id, new Path[]{hlist.get(hlist.size() - 1), blist.get(blist.size() - 1)});
        }
        return map;
    }

    private int compareDatalakePath(Path a, Path b) {
        List<String> sa = datalakeKey(a);
        List<String> sb = datalakeKey(b);
        int cmp = sa.get(0).compareTo(sb.get(0));
        if (cmp != 0) return cmp;
        cmp = sa.get(1).compareTo(sb.get(1));
        if (cmp != 0) return cmp;
        return a.getFileName().toString().compareTo(b.getFileName().toString());
    }

    private List<String> datalakeKey(Path p) {
        Path parent = p.getParent();
        Path date = parent != null ? parent.getParent() : null;
        String d = date != null ? date.getFileName().toString() : "00000000";
        String h = parent != null ? parent.getFileName().toString() : "00";
        return List.of(d, h);
    }

    private DocMeta readMeta(Path headerFile, int bookId) throws IOException {
        List<String> lines = Files.readAllLines(headerFile, StandardCharsets.UTF_8);
        DocMeta m = new DocMeta();
        m.book_id = bookId;
        m.title = "";
        m.author = "";
        m.language = "";
        m.year = 0;
        for (String ln : lines) {
            int i = ln.indexOf(':');
            if (i <= 0) continue;
            String k = ln.substring(0, i).trim().toLowerCase(Locale.ROOT);
            String v = ln.substring(i + 1).trim();
            if (k.equals("title")) m.title = v;
            else if (k.equals("author")) m.author = v;
            else if (k.equals("language")) m.language = v;
            else if (k.equals("year")) {
                try { m.year = Integer.parseInt(v.replaceAll("[^0-9]", "")); } catch (Exception ignored) {}
            }
        }
        return m;
    }

    private void updateStatus() throws IOException {
        IndexStatus st = computeStatus();
        Path statusPath = datamarts.resolve("index_status.json");
        writeAtomic(statusPath, G.toJson(st));
        Path logDir = datamarts;
        Files.createDirectories(logDir);
        Path log = logDir.resolve("indexing.log");
        String line = G.toJson(Map.of(
                "ts", Instant.now().toString(),
                "books_indexed", st.books_indexed,
                "index_size_MB", st.index_size_MB
        )) + System.lineSeparator();
        Files.writeString(log, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private IndexStatus computeStatus() throws IOException {
        Path docsDir = datamarts.resolve("docs");
        Path invDir = datamarts.resolve("inverted");
        int docs = 0;
        long bytes = 0L;
        if (Files.exists(docsDir)) {
            try (var s = Files.walk(docsDir)) {
                for (Path p : (Iterable<Path>) s::iterator) {
                    if (p.getFileName() != null && p.getFileName().toString().endsWith(".json")) {
                        docs++;
                        bytes += Files.size(p);
                    }
                }
            }
        }
        if (Files.exists(invDir)) {
            try (var s = Files.walk(invDir)) {
                for (Path p : (Iterable<Path>) s::iterator) {
                    if (p.getFileName() != null && p.getFileName().toString().endsWith(".json")) {
                        bytes += Files.size(p);
                    }
                }
            }
        }
        IndexStatus st = new IndexStatus();
        st.books_indexed = docs;
        st.last_update = Instant.now().toString();
        st.index_size_MB = Math.round((bytes / (1024.0 * 1024.0)) * 100.0) / 100.0;
        st.indexer_version = indexerVersion;
        return st;
    }

    private void writeAtomic(Path target, String content) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".tmp");
        Files.writeString(tmp, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (var s = Files.walk(path)) {
            List<Path> all = new ArrayList<>();
            for (Path p : (Iterable<Path>) s::iterator) all.add(p);
            Collections.reverse(all);
            for (Path p : all) Files.deleteIfExists(p);
        }
    }
}
