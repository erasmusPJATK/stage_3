package es.ulpgc.bd.control;

import io.javalin.Javalin;
import io.javalin.json.JavalinGson;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class ControlService {

    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(5000))
            .build();

    public static void main(String[] args) {
        int port = 7000;

        Javalin app = Javalin.create(cfg -> {
            cfg.jsonMapper(new JavalinGson());
            cfg.http.defaultContentType = "application/json";
        }).start(port);

        app.get("/status", ctx -> ctx.json(Map.of(
                "service", "control-service",
                "status", "running"
        )));

        // Minimal helper: trigger ingestion (indexing will happen asynchronously via MQ)
        // Example:
        // POST /control/ingest/123?ingestion=http://localhost:7001
        app.post("/control/ingest/{bookId}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("bookId"));
            String ingestion = ctx.queryParam("ingestion");
            if (ingestion == null || ingestion.isBlank()) ingestion = "http://localhost:7001";

            Map<String, Object> ingestResult = postJson(ingestion + "/ingest/" + bookId);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("book_id", bookId);
            response.put("ingestion", ingestion);
            response.put("ingest_result", ingestResult);
            response.put("note", "Indexing is asynchronous via ActiveMQ.");

            ctx.json(response);
        });

        System.out.println("Control Service running on http://localhost:" + port);
    }

    private static Map<String, Object> postJson(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(15000))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        String body = res.body() == null ? "" : res.body();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("http_status", res.statusCode());
        out.put("body", body);
        return out;
    }
}
