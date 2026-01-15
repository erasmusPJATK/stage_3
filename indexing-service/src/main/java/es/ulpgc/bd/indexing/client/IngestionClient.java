package es.ulpgc.bd.indexing.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class IngestionClient {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public String fetchHeader(String baseUrl, int bookId) throws Exception {
        return getText(baseUrl + "/ingest/file/" + bookId + "/header");
    }

    public String fetchBody(String baseUrl, int bookId) throws Exception {
        return getText(baseUrl + "/ingest/file/" + bookId + "/body");
    }

    public String fetchMetaOrNull(String baseUrl, int bookId) {
        try {
            return getText(baseUrl + "/ingest/file/" + bookId + "/meta");
        } catch (Exception e) {
            return null;
        }
    }

    private String getText(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 200 && res.statusCode() < 300) return res.body();
        throw new RuntimeException("HTTP " + res.statusCode() + " for " + url);
    }
}
