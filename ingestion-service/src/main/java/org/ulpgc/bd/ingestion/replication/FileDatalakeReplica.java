package org.ulpgc.bd.ingestion.replication;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class FileDatalakeReplica {
    private final HttpClient client;

    public FileDatalakeReplica() {
        this.client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public void replicate(ManifestEntry e, MqReplicationHub.LocalFiles local) {
        if (e == null || local == null) return;
        if (e.origin == null || e.origin.isBlank()) return;

        boolean ok = local.has(e.date, e.hour, e.bookId, e.sha256Header, e.sha256Body, e.sha256Meta);
        if (ok) return;

        try {
            String header = fetch(e.origin, e.bookId, "header", e.date, e.hour);
            String body = fetch(e.origin, e.bookId, "body", e.date, e.hour);
            String meta = "";
            if (e.sha256Meta != null && !e.sha256Meta.isBlank()) meta = fetch(e.origin, e.bookId, "meta", e.date, e.hour);
            local.store(e.date, e.hour, e.bookId, header, body, meta, e.sha256Header, e.sha256Body, e.sha256Meta);
        } catch (Exception ignored) {}
    }

    private String fetch(String origin, int id, String kind, String date, String hour) throws Exception {
        String base = origin.endsWith("/") ? origin.substring(0, origin.length() - 1) : origin;
        String url = base + "/ingest/file/" + id + "/" + kind + "?date=" + date + "&hour=" + hour;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) throw new RuntimeException("fetch failed: " + res.statusCode());
        return res.body() == null ? "" : res.body();
    }
}
