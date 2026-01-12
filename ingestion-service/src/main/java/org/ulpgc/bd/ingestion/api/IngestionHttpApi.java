package org.ulpgc.bd.ingestion.api;

import io.javalin.Javalin;
import org.ulpgc.bd.ingestion.service.IngestionService;

public class IngestionHttpApi {

    public static void register(Javalin app, IngestionService service) {

        app.get("/status", ctx -> ctx.json(service.status()));

        app.post("/ingest/{id}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            ctx.json(service.ingest(id));
        });

        app.get("/ingest/status/{id}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            ctx.json(service.checkStatus(id));
        });

        app.get("/ingest/list", ctx -> ctx.json(service.listBooks()));

        // Optional debug:
        app.get("/ingest/manifest", ctx -> ctx.json(service.manifest()));

        // Required for replication fetching:
        app.get("/ingest/file/{id}/{kind}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            String kind = ctx.pathParam("kind"); // header|body|meta
            String date = ctx.queryParam("date");
            String hour = ctx.queryParam("hour");

            String content = switch (kind) {
                case "header" -> service.readHeader(id, date, hour);
                case "body" -> service.readBody(id, date, hour);
                case "meta" -> service.readMeta(id, date, hour);
                default -> throw new IllegalArgumentException("Bad kind: " + kind);
            };

            ctx.contentType("text/plain; charset=utf-8");
            ctx.result(content);
        });
    }
}
