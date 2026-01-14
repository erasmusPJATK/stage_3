package es.ulpgc.bd.search.api;

import es.ulpgc.bd.search.service.SearchService;
import io.javalin.Javalin;

public class SearchHttpApi {
    private final SearchService service;

    public SearchHttpApi(SearchService service) {
        this.service = service;
    }

    public void register(Javalin app) {
        app.get("/status", ctx -> ctx.json(service.status()));
        app.get("/hz/stats", ctx -> ctx.json(service.hzStats()));

        app.get("/search", ctx -> {
            String q = ctx.queryParam("q");
            if (q == null) q = "";

            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");

            String yearStr = ctx.queryParam("year");
            Integer year = null;
            if (yearStr != null && !yearStr.isBlank()) year = Integer.parseInt(yearStr);

            String limitStr = ctx.queryParam("limit");
            int limit = 10;
            if (limitStr != null && !limitStr.isBlank()) limit = Integer.parseInt(limitStr);

            ctx.json(service.search(q, author, language, year, limit));
        });
    }
}
