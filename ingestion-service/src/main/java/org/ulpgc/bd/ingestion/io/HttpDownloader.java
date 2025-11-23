package org.ulpgc.bd.ingestion.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class HttpDownloader {
    private final String userAgent;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final HttpClient client;

    public static class FetchResult {
        public final String text;
        public final int status;
        public final int sizeBytes;
        public final String contentType;
        public FetchResult(String text, int status, int sizeBytes, String contentType) {
            this.text = text;
            this.status = status;
            this.sizeBytes = sizeBytes;
            this.contentType = contentType == null ? "" : contentType;
        }
    }

    public HttpDownloader(String userAgent, int connectTimeoutMs, int readTimeoutMs) {
        this.userAgent = userAgent;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .build();
    }

    public URL findGutenbergTextURL(int bookId) {
        List<String> patterns = List.of(
                "https://www.gutenberg.org/files/%d/%d-0.txt",
                "https://www.gutenberg.org/files/%d/%d.txt",
                "https://www.gutenberg.org/files/%d/%d-8.txt",
                "https://www.gutenberg.org/ebooks/%d.txt"
        );
        List<String> candidates = new ArrayList<>();
        for (String p : patterns) {
            if (p.contains("%d/%d")) candidates.add(String.format(p, bookId, bookId));
            else candidates.add(String.format(p, bookId));
        }
        List<CompletableFuture<Probe>> probes = new ArrayList<>();
        for (String s : candidates) {
            HttpRequest req = HttpRequest.newBuilder(URI.create(s))
                    .timeout(Duration.ofMillis(readTimeoutMs))
                    .header("User-Agent", userAgent)
                    .header("Accept-Encoding", "gzip")
                    .header("Range", "bytes=0-0")
                    .GET()
                    .build();
            CompletableFuture<Probe> fut = client.sendAsync(req, HttpResponse.BodyHandlers.discarding())
                    .handle((resp, ex) -> {
                        if (ex != null) return new Probe(s, 0, "");
                        int code = resp.statusCode();
                        String ct = header(resp.headers(), "Content-Type").orElse("");
                        return new Probe(s, code, ct);
                    });
            probes.add(fut);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(readTimeoutMs + 1000);
        List<Probe> results = new ArrayList<>();
        for (CompletableFuture<Probe> f : probes) {
            long left = deadline - System.nanoTime();
            if (left <= 0) break;
            try {
                results.add(f.get(Math.min(left, TimeUnit.SECONDS.toNanos(2)), TimeUnit.NANOSECONDS));
            } catch (Exception ignored) {}
        }
        results.sort(Comparator.comparingInt(p -> p.rank()));
        for (Probe p : results) {
            if (p.isOk()) {
                try {
                    return new URL(p.url);
                } catch (Exception ignored) {}
            }
        }
        for (String s : candidates) {
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create(s))
                        .timeout(Duration.ofMillis(readTimeoutMs))
                        .header("User-Agent", userAgent)
                        .header("Accept-Encoding", "gzip")
                        .method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .build();
                HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
                if (resp.statusCode() == 200) return new URL(s);
            } catch (Exception ignored) {}
        }
        return null;
    }

    public String fetchText(URL url) throws IOException {
        FetchResult r = fetchTextWithInfo(url);
        return r.text;
    }

    public FetchResult fetchTextWithInfo(URL url) throws IOException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url.toString()))
                .timeout(Duration.ofMillis(readTimeoutMs))
                .header("User-Agent", userAgent)
                .header("Accept-Encoding", "gzip")
                .GET()
                .build();
        try {
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            int code = resp.statusCode();
            byte[] raw = resp.body() == null ? new byte[0] : resp.body();
            HttpHeaders h = resp.headers();
            String ct = header(h, "Content-Type").orElse("");
            String ce = header(h, "Content-Encoding").orElse("");
            byte[] bytes = ce.toLowerCase(Locale.ROOT).contains("gzip") ? ungzip(raw) : raw;
            int size = bytes.length;
            String charset = detectCharset(ct);
            String text;
            try {
                text = new String(bytes, Charset.forName(charset));
            } catch (Exception e) {
                try { text = new String(bytes, StandardCharsets.UTF_8); }
                catch (Exception e2) { text = new String(bytes, Charset.forName("ISO-8859-1")); }
            }
            return new FetchResult(text, code, size, ct);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    private static Optional<String> header(HttpHeaders headers, String name) {
        return headers.firstValue(name);
    }

    private static String detectCharset(String contentType) {
        if (contentType == null) return "UTF-8";
        Matcher m = Pattern.compile("(?i)charset=([\\w\\-]+)").matcher(contentType);
        if (m.find()) return m.group(1).trim();
        return "UTF-8";
    }

    private static byte[] ungzip(byte[] gz) throws IOException {
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(gz))) {
            return gis.readAllBytes();
        }
    }

    private static class Probe {
        final String url;
        final int code;
        final String ct;
        Probe(String url, int code, String ct) { this.url = url; this.code = code; this.ct = ct == null ? "" : ct; }
        boolean isOk() { return code == 200 || code == 206; }
        int rank() {
            if (isOk()) return 0;
            if (code >= 300 && code < 400) return 5;
            if (code >= 400 && code < 500) return 10;
            if (code >= 500) return 15;
            return 20;
        }
    }
}
