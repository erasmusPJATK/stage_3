package org.ulpgc.bd.search.api;

import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.ulpgc.bd.search.model.SearchResult;
import org.ulpgc.bd.search.service.SearchService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SearchHttpApi {

    private static final Gson gson = new Gson();

    public static void register(Javalin app, SearchService service) {
        app.get("/status", ctx -> ctx.json(Map.of("service", "search-service", "status", "running")));
        app.get("/search", ctx -> handleSearch(ctx, service));
        app.post("/refresh", ctx -> ctx.json(service.refresh()));
    }

    private static void handleSearch(Context ctx, SearchService service) {
        String q = ctx.queryParam("q");
        String author = ctx.queryParam("author");
        String language = ctx.queryParam("language");
        String yearStr = ctx.queryParam("year");
        Integer year = null;
        try { if (yearStr != null && !yearStr.isBlank()) year = Integer.parseInt(yearStr.trim()); } catch (Exception ignored) {}
        int k = ctx.queryParamAsClass("k", Integer.class).getOrDefault(10);

        List<SearchResult> results = service.search(q, author, language, year, k);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("query", q == null ? "" : q);
        response.put("filters", buildFilters(author, language, year));
        response.put("count", results.size());
        response.put("results", results);

        ctx.result(gson.toJson(response));
    }

    private static Map<String, Object> buildFilters(String author, String language, Integer year) {
        Map<String, Object> filters = new LinkedHashMap<>();
        if (author != null && !author.isBlank()) filters.put("author", author);
        if (language != null && !language.isBlank()) filters.put("language", language);
        if (year != null && year > 0) filters.put("year", year);
        return filters;
    }
}
