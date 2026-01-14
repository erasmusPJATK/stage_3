package es.ulpgc.bd.control;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.javalin.Javalin;
import io.javalin.json.JavalinGson;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class ControlService {

    private static final String INGEST_BASE = "http://localhost:7001";
    private static final String INDEX_BASE = "http://localhost:7002";
    private static final String SEARCH_BASE = "http://localhost:7003";

    private static final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofMillis(5000))
            .build();

    private static final Gson gson = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

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

        app.post("/control/run/{book_id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            try {
                Map<String, Object> ingestResult = postJson(INGEST_BASE + "/ingest/" + bookId);
                Map<String, Object> ingestStatus = getJson(INGEST_BASE + "/ingest/status/" + bookId);
                Map<String, Object> indexResult = postJson(INDEX_BASE + "/index/update/" + bookId);
                Map<String, Object> indexStatus = getJson(INDEX_BASE + "/index/status");
                Map<String, Object> searchRefresh = postJson(SEARCH_BASE + "/refresh");

                Map<String, Object> response = new LinkedHashMap<>();
                response.put("book_id", bookId);
                response.put("ingest", Map.of(
                        "request_result", ingestResult,
                        "status_check", ingestStatus
                ));
                response.put("index", Map.of(
                        "update_result", indexResult,
                        "global_status", indexStatus
                ));
                response.put("search_refresh", searchRefresh);
                response.put("workflow_status", "complete");

                ctx.json(response);
            } catch (Exception e) {
                ctx.status(500).json(Map.of(
                        "workflow_status", "error",
                        "error", e.getMessage()
                ));
            }
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
        if (body.isEmpty()) {
            return Map.of("http_status", res.statusCode());
        }
        Map<String, Object> map = gson.fromJson(body, MAP_TYPE);
        if (map == null) {
            map = new LinkedHashMap<>();
        }
        map.putIfAbsent("http_status", res.statusCode());
        return map;
    }

    private static Map<String, Object> getJson(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(15000))
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        String body = res.body() == null ? "" : res.body();
        if (body.isEmpty()) {
            return Map.of("http_status", res.statusCode());
        }
        Map<String, Object> map = gson.fromJson(body, MAP_TYPE);
        if (map == null) {
            map = new LinkedHashMap<>();
        }
        map.putIfAbsent("http_status", res.statusCode());
        return map;
    }
}
