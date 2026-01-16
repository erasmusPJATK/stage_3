package es.ulpgc.bd.ingestion.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import es.ulpgc.bd.ingestion.io.HttpDownloader;
import es.ulpgc.bd.ingestion.model.Meta;
import es.ulpgc.bd.ingestion.mq.MqProducer;
import es.ulpgc.bd.ingestion.parser.GutenbergMetaExtractor;
import es.ulpgc.bd.ingestion.parser.GutenbergSplitter;
import es.ulpgc.bd.ingestion.replication.DatalakeScanner;
import es.ulpgc.bd.ingestion.replication.ManifestEntry;
import es.ulpgc.bd.ingestion.replication.MqReplicationHub;
import es.ulpgc.bd.ingestion.replication.ReplicationEvent;

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
import java.util.stream.Stream;

public class IngestionService implements MqReplicationHub.LocalFiles {

    private final Path datalake;
    private final String parserVersion;
    private final HttpDownloader downloader;
    private final GutenbergSplitter splitter;
    private final GutenbergMetaExtractor extractor;

    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();

    private volatile MqReplicationHub hub;
    private volatile String origin = "";
    private volatile String mq = "";

    private volatile MqProducer indexingProducer;
    private volatile String indexingQueueName = "ingestion.ingested";

    public IngestionService(Path datalake, String parserVersion, HttpDownloader downloader, GutenbergSplitter splitter, GutenbergMetaExtractor extractor) {
        this.datalake = datalake;
        this.parserVersion = parserVersion;
        this.downloader = downloader;
        this.splitter = splitter;
        this.extractor = extractor;
    }

    public void setReplicationHub(MqReplicationHub hub) {
        this.hub = hub;
    }

    public void setOrigin(String origin) {
        this.origin = origin == null ? "" : origin;
    }

    public void setMq(String mq) {
        this.mq = mq == null ? "" : mq;
    }

    public String getOrigin() {
        return origin;
    }

    public void setIndexingProducer(MqProducer producer, String queueName) {
        this.indexingProducer = producer;
        if (queueName != null && !queueName.isBlank()) {
            this.indexingQueueName = queueName.trim();
        }
    }

    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("service", "ingestion");
        m.put("datalake", datalake.toString());
        m.put("parser_version", parserVersion);
        m.put("origin", origin);
        m.put("mq", mq);
        m.put("indexingQueue", indexingQueueName);
        m.put("indexingMqEnabled", indexingProducer != null);
        return m;
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
            Path metaPath = dir.resolve(bookId + "_meta.json");

            String header = "Title: " + meta.title + System.lineSeparator()
                    + "Author: " + meta.author + System.lineSeparator()
                    + "Language: " + meta.language;

            writeAtomic(headerPath, header);
            writeAtomic(bodyPath, body);

            String shaBody = sha256Hex(Files.readAllBytes(bodyPath));
            String shaHeader = sha256Hex(Files.readAllBytes(headerPath));

            persistMetaJson(metaPath, bookId, meta, url.toString(), shaBody, parserVersion);
            String shaMeta = Files.exists(metaPath) ? sha256Hex(Files.readAllBytes(metaPath)) : "";

            long t3 = System.nanoTime();

            ReplicationEvent ev = new ReplicationEvent();
            ev.type = "INGESTED";
            ev.origin = origin;
            ev.bookId = bookId;
            ev.date = date;
            ev.hour = hour;
            ev.sha256Header = shaHeader;
            ev.sha256Body = shaBody;
            ev.sha256Meta = shaMeta;
            ev.parserVersion = parserVersion;

            String replStatus = "skipped";
            String replMessage = null;

            try {
                if (hub != null) {
                    hub.publishIngested(ev);
                    replStatus = "ok";
                }
            } catch (Exception ex) {
                replStatus = "error";
                replMessage = ex.getMessage();
            }

            String indexingStatus = "skipped";
            String indexingMessage = null;

            try {
                if (indexingProducer != null) {
                    indexingProducer.publish(bookId, origin);
                    indexingStatus = "ok";
                }
            } catch (Exception ex) {
                indexingStatus = "error";
                indexingMessage = ex.getMessage();
            }

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
            response.put("checksum_sha256_body", shaBody);
            response.put("parser_version", parserVersion);
            response.put("ingested_at", LocalDateTime.now().toString());

            response.put("replication_publish", replStatus);
            if (replMessage != null && !replMessage.isBlank()) response.put("replication_message", replMessage);

            response.put("indexing_event_publish", indexingStatus);
            response.put("indexing_queue", indexingQueueName);
            if (indexingMessage != null && !indexingMessage.isBlank()) response.put("indexing_message", indexingMessage);

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
                Stream<Path> stream = Files.walk(datalake);
                try {
                    Iterator<Path> it = stream.iterator();
                    while (it.hasNext()) {
                        Path p = it.next();
                        String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                        if (fn.equals(bookId + "_header.txt")) headerFound = true;
                        if (fn.equals(bookId + "_body.txt")) bodyFound = true;
                        if (headerFound && bodyFound) break;
                    }
                } finally {
                    stream.close();
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
                Stream<Path> stream = Files.walk(datalake);
                try {
                    for (Iterator<Path> it = stream.iterator(); it.hasNext(); ) {
                        Path p = it.next();
                        String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                        Matcher m1 = Pattern.compile("^(\\d+)_header\\.txt$").matcher(fn);
                        Matcher m2 = Pattern.compile("^(\\d+)_body\\.txt$").matcher(fn);
                        if (m1.matches()) ids.add(Integer.parseInt(m1.group(1)));
                        else if (m2.matches()) ids.add(Integer.parseInt(m2.group(1)));
                    }
                } finally {
                    stream.close();
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

    public List<ManifestEntry> manifest() {
        DatalakeScanner s = new DatalakeScanner(datalake);
        Map<Integer, Path[]> map = s.latestHeaderBodyMetaByBook();
        List<ManifestEntry> out = new ArrayList<>();
        for (Map.Entry<Integer, Path[]> e : map.entrySet()) {
            int id = e.getKey();
            Path h = e.getValue()[0];
            Path b = e.getValue()[1];
            Path m = e.getValue()[2];

            String[] dh = DatalakeScanner.dateHourOf(h);

            ManifestEntry me = new ManifestEntry();
            me.bookId = id;
            me.date = dh[0];
            me.hour = dh[1];
            try { me.sha256Header = sha256Hex(Files.readAllBytes(h)); } catch (Exception ex) { me.sha256Header = ""; }
            try { me.sha256Body = sha256Hex(Files.readAllBytes(b)); } catch (Exception ex) { me.sha256Body = ""; }
            try { me.sha256Meta = (m != null && Files.exists(m)) ? sha256Hex(Files.readAllBytes(m)) : ""; } catch (Exception ex) { me.sha256Meta = ""; }
            me.origin = origin;
            me.parserVersion = parserVersion;
            out.add(me);
        }
        out.sort(Comparator.comparingInt(a -> a.bookId));
        return out;
    }

    public String readHeader(int bookId, String date, String hour) throws Exception {
        Path p = resolveFile(bookId, "header", date, hour);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    public String readBody(int bookId, String date, String hour) throws Exception {
        Path p = resolveFile(bookId, "body", date, hour);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    public String readMeta(int bookId, String date, String hour) throws Exception {
        Path p = resolveFile(bookId, "meta", date, hour);
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private Path resolveFile(int bookId, String kind, String date, String hour) throws Exception {
        if (date != null && hour != null) {
            Path dir = datalake.resolve(date).resolve(hour);
            Path p = null;
            if ("header".equals(kind)) p = dir.resolve(bookId + "_header.txt");
            else if ("body".equals(kind)) p = dir.resolve(bookId + "_body.txt");
            else if ("meta".equals(kind)) p = dir.resolve(bookId + "_meta.json");
            if (p != null && Files.exists(p)) return p;
        }

        Path[] pair = findLatest(bookId);
        if (pair == null) throw new IOException("book not found");

        if ("header".equals(kind)) return pair[0];
        if ("body".equals(kind)) return pair[1];
        if ("meta".equals(kind)) {
            if (pair[2] != null) return pair[2];
            throw new IOException("meta not found");
        }
        throw new IOException("bad kind");
    }

    private Path[] findLatest(int bookId) throws IOException {
        DatalakeScanner s = new DatalakeScanner(datalake);
        Map<Integer, Path[]> map = s.latestHeaderBodyMetaByBook();
        return map.get(bookId);
    }

    private void persistMetaJson(Path out, int bookId, Meta meta, String sourceUrl, String sha256Body, String parserVersion) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("book_id", bookId);
        m.put("title", meta.title);
        m.put("author", meta.author);
        m.put("language", meta.language);
        m.put("source_url", sourceUrl);
        m.put("checksum_sha256_body", sha256Body);
        m.put("parser_version", parserVersion);
        m.put("ingested_at", LocalDateTime.now().toString());
        try {
            writeAtomic(out, G.toJson(m));
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

    @Override
    public boolean has(String date, String hour, int bookId, String shaHeader, String shaBody, String shaMeta) {
        try {
            Path dir = datalake.resolve(date).resolve(hour);
            Path h = dir.resolve(bookId + "_header.txt");
            Path b = dir.resolve(bookId + "_body.txt");
            Path m = dir.resolve(bookId + "_meta.json");
            if (!Files.exists(h) || !Files.exists(b)) return false;

            if (shaHeader != null && !shaHeader.trim().isEmpty()) {
                String sh = sha256Hex(Files.readAllBytes(h));
                if (!shaHeader.equals(sh)) return false;
            }
            if (shaBody != null && !shaBody.trim().isEmpty()) {
                String sb = sha256Hex(Files.readAllBytes(b));
                if (!shaBody.equals(sb)) return false;
            }
            if (shaMeta != null && !shaMeta.trim().isEmpty() && Files.exists(m)) {
                String sm = sha256Hex(Files.readAllBytes(m));
                if (!shaMeta.equals(sm)) return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void store(String date, String hour, int bookId, String header, String body, String metaJson, String shaHeader, String shaBody, String shaMeta) throws Exception {
        Path dir = datalake.resolve(date).resolve(hour);
        Files.createDirectories(dir);
        Path h = dir.resolve(bookId + "_header.txt");
        Path b = dir.resolve(bookId + "_body.txt");
        Path m = dir.resolve(bookId + "_meta.json");
        writeAtomic(h, header == null ? "" : header);
        writeAtomic(b, body == null ? "" : body);
        if (metaJson != null && !metaJson.trim().isEmpty()) writeAtomic(m, metaJson);
    }
}
