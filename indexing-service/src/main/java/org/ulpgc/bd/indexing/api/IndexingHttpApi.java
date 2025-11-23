package org.ulpgc.bd.indexing.api;

import io.javalin.Javalin;
import org.ulpgc.bd.indexing.service.IndexingService;

public class IndexingHttpApi {
    public static void register(Javalin app, IndexingService service) {
        app.post("/index/update/{book_id}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("book_id"));
            ctx.json(service.updateOne(id));
        });
        app.post("/index/rebuild", ctx -> ctx.json(service.rebuildAll()));
        app.get("/index/status", ctx -> ctx.json(service.status()));
    }
}
