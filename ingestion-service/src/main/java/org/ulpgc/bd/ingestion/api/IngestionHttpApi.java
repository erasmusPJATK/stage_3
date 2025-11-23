package org.ulpgc.bd.ingestion.api;

import io.javalin.Javalin;
import org.ulpgc.bd.ingestion.service.IngestionService;

import java.util.Map;

public class IngestionHttpApi {

    public static void register(Javalin app, IngestionService service) {
        app.post("/ingest/{id}", ctx -> {
            try {
                int id = Integer.parseInt(ctx.pathParam("id"));
                Map<String, Object> res = service.ingest(id);
                int code = "error".equalsIgnoreCase(String.valueOf(res.get("status"))) ? 500 : 200;
                ctx.status(code).json(res);
            } catch (NumberFormatException nfe) {
                ctx.status(400).json(Map.of("status", "error", "message", "invalid book_id"));
            }
        });

        app.get("/ingest/status/{id}", ctx -> {
            try {
                int id = Integer.parseInt(ctx.pathParam("id"));
                Map<String, Object> res = service.checkStatus(id);
                int code = "error".equalsIgnoreCase(String.valueOf(res.get("status"))) ? 500 : 200;
                ctx.status(code).json(res);
            } catch (NumberFormatException nfe) {
                ctx.status(400).json(Map.of("status", "error", "message", "invalid book_id"));
            }
        });

        app.get("/ingest/list", ctx -> ctx.status(200).json(service.listBooks()));
    }
}
