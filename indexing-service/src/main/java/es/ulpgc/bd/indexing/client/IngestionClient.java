package es.ulpgc.bd.indexing.client;

import com.google.gson.Gson;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IngestionClient {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final Gson gson = new Gson();

    public String fetchHeader(String baseUrl, int bookId) throws Exception {
        return getText(norm(baseUrl) + "/ingest/file/" + bookId + "/header");
    }

    public String fetchBody(String baseUrl, int bookId) throws Exception {
        return getText(norm(baseUrl) + "/ingest/file/" + bookId + "/body");
    }

    public String fetchMetaOrNull(String baseUrl, int bookId) {
        try {
            return getText(norm(baseUrl) + "/ingest/file/" + bookId + "/meta");
        } catch (Exception e) {
            return null;
        }
    }

    public List<Integer> listBooks(String baseUrl) throws Exception {
        String json = getText(norm(baseUrl) + "/ingest/list");
        @SuppressWarnings("unchecked")
        Map<String, Object> m = gson.fromJson(json, Map.class);
        Object b = m.get("books");
        List<Integer> out = new ArrayList<>();
        if (b instanceof List<?> list) {
            for (Object x : list) {
                if (x instanceof Number n) out.add(n.intValue());
                else if (x != null) {
                    try { out.add(Integer.parseInt(x.toString().trim())); } catch (Exception ignored) {}
                }
            }
        }
        return out;
    }

    private String getText(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(12))
                .GET()
                .build();

        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 200 && res.statusCode() < 300) return res.body();
        throw new RuntimeException("HTTP " + res.statusCode() + " for " + url);
    }

    private static String norm(String baseUrl) {
        if (baseUrl == null) return "";
        String t = baseUrl.trim();
        if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
        return t;
    }
}
