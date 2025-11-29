package org.ulpgc.bd.ingestion;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class IngestionServiceApp {
    public static void main(String[] args) throws Exception {
        Args a = new Args(args);
        int port = a.getInt("port",7001);
        String broker = a.get("mq","tcp://localhost:61616");
        String datalake = a.get("datalake","datalake");
        String origin = a.get("origin","http://localhost:"+port);
        Files.createDirectories(Paths.get(datalake));
        MqPublisher pub = new MqPublisher(broker,"books.ingested");
        Javalin app = Javalin.create(c -> c.http.defaultContentType="application/json").start(port);

        app.post("/ingest/{id}", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
            String hour = String.format("%02d", LocalTime.now().getHour());
            Path dir = Paths.get(datalake, date, hour);
            Files.createDirectories(dir);
            Path header = dir.resolve(id+"_header.txt");
            Path body = dir.resolve(id+"_body.txt");
            String h = "Title: Sample "+id+"\nAuthor: Unknown\nLanguage: English\nYear: 1900";
            String b = "Document "+id+" sample text for distributed MVP search indexing.";
            Files.writeString(header,h, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.writeString(body,b, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            pub.publish(Map.of("bookId", id, "origin", origin));
            ctx.json(Map.of("book_id", id, "status", "ingested", "path", dir.toString(), "origin", origin));
        });

        app.get("/ingest/file/{id}/header", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            Path p = findLatest(datalake, id, "_header.txt");
            if(p==null){ ctx.status(404).result("not found"); return; }
            ctx.contentType("text/plain; charset=utf-8").result(Files.readString(p));
        });

        app.get("/ingest/file/{id}/body", ctx -> {
            int id = Integer.parseInt(ctx.pathParam("id"));
            Path p = findLatest(datalake, id, "_body.txt");
            if(p==null){ ctx.status(404).result("not found"); return; }
            ctx.contentType("text/plain; charset=utf-8").result(Files.readString(p));
        });

        app.get("/status", ctx -> ctx.json(Map.of("service","ingestion","port",port,"mq",broker,"origin",origin)));
        Runtime.getRuntime().addShutdownHook(new Thread(pub::close));
    }

    private static Path findLatest(String root, int id, String suffix) throws Exception {
        Path base = Paths.get(root);
        if(!Files.exists(base)) return null;
        Path[] best = new Path[1];
        try(var s = Files.walk(base)){
            s.filter(p->p.getFileName()!=null && p.getFileName().toString().equals(id+suffix)).forEach(p-> best[0]=p);
        }
        return best[0];
    }
}
