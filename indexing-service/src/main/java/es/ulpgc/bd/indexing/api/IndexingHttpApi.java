package es.ulpgc.bd.indexing.api;

import es.ulpgc.bd.indexing.service.IndexingService;
import io.javalin.Javalin;

import java.util.LinkedHashMap;
import java.util.Map;

public class IndexingHttpApi {

    public static void register(Javalin app, IndexingService svc) {
        register(app, svc, null, null, null, null);
    }

    public static void register(Javalin app, IndexingService svc, String mq, Integer port, String hzMembers, String ingestionBase) {

        app.get("/status", ctx -> {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("service", "indexing");
            if (port != null) s.put("port", port);
            if (mq != null) s.put("mq", mq);
            if (hzMembers != null) s.put("hzMembers", hzMembers);
            if (ingestionBase != null) s.put("ingestion", ingestionBase);
            s.putAll(svc.stats());
            ctx.json(s);
        });

        app.post("/index/update/{bookId}", ctx -> {
            final int bookId = Integer.parseInt(ctx.pathParam("bookId"));
            final String origin = ctx.queryParam("origin");
            final String base = (origin != null && !origin.isBlank()) ? origin : ingestionBase;

            ctx.json(svc.update(bookId, base));
        });
    }
}
