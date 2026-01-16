package es.ulpgc.bd.search.api;

import es.ulpgc.bd.search.service.SearchService;
import io.javalin.Javalin;

import java.util.List;
import java.util.Map;

public class SearchHttpApi {
    private final SearchService service;

    public SearchHttpApi(SearchService service) {
        this.service = service;
    }

    public void register(Javalin app) {

        // Health endpoints for load balancer checks
        app.get("/health", ctx -> ctx.json(Map.of("status", "UP")));
        app.get("/ready", ctx -> {
            if (service.isReady()) ctx.json(Map.of("ready", true));
            else ctx.status(503).json(Map.of("ready", false));
        });

        app.get("/status", ctx -> ctx.json(service.status()));
        app.get("/hz/stats", ctx -> ctx.json(service.hzStats()));

        app.get("/search", ctx -> {
            String q = ctx.queryParam("q");
            if (q == null) q = "";

            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");

            String yearStr = ctx.queryParam("year");
            Integer year = null;
            if (yearStr != null && !yearStr.isBlank()) {
                try {
                    year = Integer.parseInt(yearStr);
                } catch (NumberFormatException e) {
                    ctx.status(400).json(Map.of("error", "Invalid year parameter"));
                    return;
                }
            }

            String limitStr = ctx.queryParam("limit");
            String kStr = ctx.queryParam("k");
            int limit = 10;
            if (limitStr != null && !limitStr.isBlank()) {
                try {
                    limit = Integer.parseInt(limitStr);
                } catch (NumberFormatException e) {
                    ctx.status(400).json(Map.of("error", "Invalid limit parameter"));
                    return;
                }
            } else if (kStr != null && !kStr.isBlank()) {
                try {
                    limit = Integer.parseInt(kStr);
                } catch (NumberFormatException e) {
                    ctx.status(400).json(Map.of("error", "Invalid k parameter"));
                    return;
                }
            }

            List<Map<String, Object>> results = service.search(q, author, language, year, limit);

            Map<String, Object> response = new java.util.LinkedHashMap<>();
            response.put("query", q.trim());
            Map<String, Object> filters = new java.util.LinkedHashMap<>();
            if (author != null && !author.isBlank()) filters.put("author", author);
            if (language != null && !language.isBlank()) filters.put("language", language);
            if (year != null) filters.put("year", year);
            response.put("filters", filters);
            response.put("count", results.size());
            response.put("results", results);

            ctx.json(response);
        });

        app.get("/hz/members", ctx -> ctx.json(service.hzStats().get("members")));
    }
}
