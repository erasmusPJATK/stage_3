package org.ulpgc.bd.ingestion.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.ulpgc.bd.ingestion.io.HttpDownloader;
import org.ulpgc.bd.ingestion.model.Meta;
import org.ulpgc.bd.ingestion.parser.GutenbergMetaExtractor;
import org.ulpgc.bd.ingestion.parser.GutenbergSplitter;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IngestionService {

    private final Path datalake;
    private final String parserVersion;
    private final HttpDownloader downloader;
    private final GutenbergSplitter splitter;
    private final GutenbergMetaExtractor extractor;
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public IngestionService(Path datalake, String parserVersion, HttpDownloader downloader, GutenbergSplitter splitter, GutenbergMetaExtractor extractor) {
        this.datalake = datalake;
        this.parserVersion = parserVersion;
        this.downloader = downloader;
        this.splitter = splitter;
        this.extractor = extractor;
    }

    public Map<String, Object> ingest(int bookId) {
        Map<String, Object> response = new LinkedHashMap<>();
        long t0 = System.nanoTime();
        try {
            String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
            String hour = String.format("%02d", LocalTime.now().getHour());
            Path dir = datalake.resolve(date).resolve(hour);
            Files.createDirectories(dir);

            URL url = downloader.findGutenbergTextURL(bookId);
            if (url == null) throw new IOException("No accessible plain-text URL found for bookId " + bookId);

            long t1 = System.nanoTime();
            HttpDownloader.FetchResult fr = downloader.fetchTextWithInfo(url);
            long t2 = System.nanoTime();

            GutenbergSplitter.Markers markers = splitter.findMarkers(fr.text);
            String body = splitter.sliceBody(fr.text, markers);
            Meta meta = extractor.extract(fr.text, markers);

            Path headerPath = dir.resolve(bookId + "_header.txt");
            Path bodyPath = dir.resolve(bookId + "_body.txt");

            String header = "Title: " + meta.title + System.lineSeparator()
                    + "Author: " + meta.author + System.lineSeparator()
                    + "Language: " + meta.language + System.lineSeparator()
                    + "Year: " + (meta.year > 0 ? String.valueOf(meta.year) : "");

            writeAtomic(headerPath, header);
            writeAtomic(bodyPath, body);

            String sha256 = Files.exists(bodyPath) ? sha256Hex(Files.readAllBytes(bodyPath)) : "";

            persistMetaJson(dir, bookId, meta, url.toString(), sha256, parserVersion);

            long t3 = System.nanoTime();
            long resolveMs = (t1 - t0) / 1_000_000L;
            long downloadMs = (t2 - t1) / 1_000_000L;
            long parseMs = (t3 - t2) / 1_000_000L;

            response.put("book_id", bookId);
            response.put("status", "downloaded");
            response.put("path", dir.toString());
            response.put("source_url", url.toString());
            response.put("http_status", fr.status);
            response.put("size_bytes", fr.sizeBytes);
            response.put("resolve_ms", resolveMs);
            response.put("download_ms", downloadMs);
            response.put("parse_ms", parseMs);
            response.put("title", meta.title);
            response.put("author", meta.author);
            response.put("language", meta.language);
            response.put("year", meta.year);
            response.put("checksum_sha256", sha256);
            response.put("parser_version", parserVersion);
            response.put("ingested_at", LocalDateTime.now().toString());

            persistIngestionLog(response);

        } catch (Exception e) {
            response.clear();
            response.put("book_id", bookId);
            response.put("status", "error");
            response.put("message", e.getMessage());
        }
        return response;
    }

    public Map<String, Object> checkStatus(int bookId) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            boolean headerFound = false;
            boolean bodyFound = false;
            if (Files.exists(datalake)) {
                try (var stream = Files.walk(datalake)) {
                    for (Path p : (Iterable<Path>) stream::iterator) {
                        String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                        if (fn.equals(bookId + "_header.txt")) headerFound = true;
                        if (fn.equals(bookId + "_body.txt")) bodyFound = true;
                        if (headerFound && bodyFound) break;
                    }
                }
            }
            response.put("book_id", bookId);
            response.put("header", headerFound ? "present" : "missing");
            response.put("body", bodyFound ? "present" : "missing");
            response.put("status", bodyFound ? "available" : "not found");
        } catch (IOException e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
        }
        return response;
    }

    public Map<String, Object> listBooks() {
        Map<String, Object> response = new LinkedHashMap<>();
        Set<Integer> ids = new HashSet<>();
        try {
            if (Files.exists(datalake)) {
                try (var stream = Files.walk(datalake)) {
                    for (Path p : (Iterable<Path>) stream::iterator) {
                        String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                        Matcher m1 = Pattern.compile("^(\\d+)_header\\.txt$").matcher(fn);
                        Matcher m2 = Pattern.compile("^(\\d+)_body\\.txt$").matcher(fn);
                        if (m1.matches()) ids.add(Integer.parseInt(m1.group(1)));
                        else if (m2.matches()) ids.add(Integer.parseInt(m2.group(1)));
                    }
                }
            }
            List<Integer> out = new ArrayList<>(ids);
            Collections.sort(out);
            response.put("count", out.size());
            response.put("books", out);
        } catch (IOException e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
        }
        return response;
    }

    private void persistMetaJson(Path dir, int bookId, Meta meta, String sourceUrl, String sha256, String parserVersion) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("book_id", bookId);
        m.put("title", meta.title);
        m.put("author", meta.author);
        m.put("language", meta.language);
        m.put("year", meta.year);
        m.put("source_url", sourceUrl);
        m.put("checksum_sha256", sha256);
        m.put("parser_version", parserVersion);
        m.put("ingested_at", LocalDateTime.now().toString());
        Path out = dir.resolve(bookId + "_meta.json");
        try {
            writeAtomic(out, GSON.toJson(m));
        } catch (IOException ignored) {}
    }

    private void persistIngestionLog(Map<String, Object> payload) {
        try {
            Files.createDirectories(datalake);
            Path log = datalake.resolve("ingestion.log");
            String line = GSON.toJson(payload) + System.lineSeparator();
            Files.writeString(log, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    private static void writeAtomic(Path target, String content) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".tmp");
        Files.writeString(tmp, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
