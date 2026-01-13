package org.ulpgc.bd.ingestion.api;

import io.javalin.Javalin;
import org.ulpgc.bd.ingestion.replication.ManifestEntry;
import org.ulpgc.bd.ingestion.service.IngestionService;

import java.util.List;

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

        app.get("/ingest/manifest", ctx -> {
            List<ManifestEntry> m = service.manifest();
            ctx.json(m);
        });

        app.get("/ingest/file/{id}/{kind}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            String kind = ctx.pathParam("kind");
            String date = ctx.queryParam("date");
            String hour = ctx.queryParam("hour");

            try {
                String out = switch (kind) {
                    case "header" -> service.readHeader(id, date, hour);
                    case "body" -> service.readBody(id, date, hour);
                    case "meta" -> service.readMeta(id, date, hour);
                    default -> null;
                };
                if (out == null) {
                    ctx.status(400).result("bad kind");
                } else {
                    ctx.result(out);
                }
            } catch (Exception e) {
                ctx.status(404).result("not found");
            }
        });
    }
}
