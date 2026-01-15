package es.ulpgc.bd.ingestion.api;

import io.javalin.Javalin;
import io.javalin.http.Context;
import es.ulpgc.bd.ingestion.replication.MqReplicationHub;
import es.ulpgc.bd.ingestion.service.IngestionService;

public class IngestionHttpApi {

    public static void register(Javalin app, IngestionService service, MqReplicationHub hub) {

        app.get("/status", ctx -> ctx.json(service.status()));

        app.post("/ingest/{bookId}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("bookId"));
            ctx.json(service.ingest(id));
        });

        app.get("/ingest/status/{bookId}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("bookId"));
            ctx.json(service.checkStatus(id));
        });

        app.get("/ingest/list", ctx -> ctx.json(service.listBooks()));

        app.get("/ingest/manifest", ctx -> ctx.json(service.manifest()));

        app.get("/ingest/file/{bookId}/{kind}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("bookId"));
            String kind = ctx.pathParam("kind");
            String date = qp(ctx, "date");
            String hour = qp(ctx, "hour");

            String out;
            if ("header".equals(kind)) out = service.readHeader(id, date, hour);
            else if ("body".equals(kind)) out = service.readBody(id, date, hour);
            else if ("meta".equals(kind)) out = service.readMeta(id, date, hour);
            else out = "";

            if (out == null || out.isEmpty()) ctx.status(404).result("not found");
            else ctx.result(out);
        });

        app.get("/repl/state", ctx -> ctx.json(hub.state()));
    }

    private static String qp(Context ctx, String k) {
        String v = ctx.queryParam(k);
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
