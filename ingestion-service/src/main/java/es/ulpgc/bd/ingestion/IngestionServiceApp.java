package es.ulpgc.bd.ingestion;

import io.javalin.Javalin;
import io.javalin.json.JavalinGson;
import es.ulpgc.bd.ingestion.api.IngestionHttpApi;
import es.ulpgc.bd.ingestion.io.HttpDownloader;
import es.ulpgc.bd.ingestion.parser.GutenbergMetaExtractor;
import es.ulpgc.bd.ingestion.parser.GutenbergSplitter;
import es.ulpgc.bd.ingestion.replication.MqReplicationHub;
import es.ulpgc.bd.ingestion.service.IngestionService;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class IngestionServiceApp {

    public static void main(String[] args) throws Exception {
        Map<String, String> a = Args.parse(args);

        int port = Integer.parseInt(a.getOrDefault("port", "7001"));
        String mq = a.getOrDefault("mq", "tcp://localhost:61616");
        String origin = a.getOrDefault("origin", "http://localhost:" + port);

        Path moduleRoot = detectModuleRoot(IngestionServiceApp.class);
        Path datalake = moduleRoot.resolve("datalake").toAbsolutePath().normalize();
        String parserVersion = a.getOrDefault("parser", "gutenberg-heuristics-8");

        HttpDownloader downloader = new HttpDownloader("IngestionService/3.0", 6000, 10000);
        GutenbergSplitter splitter = new GutenbergSplitter();
        GutenbergMetaExtractor extractor = new GutenbergMetaExtractor();

        IngestionService service = new IngestionService(datalake, parserVersion, downloader, splitter, extractor);
        service.setOrigin(origin);
        service.setMq(mq);

        MqReplicationHub hub = new MqReplicationHub(mq, origin, service);
        hub.start();
        service.setReplicationHub(hub);

        Javalin app = Javalin.create(cfg -> cfg.jsonMapper(new JavalinGson())).start(port);
        IngestionHttpApi.register(app, service, hub);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { hub.close(); } catch (Exception ignored) {}
        }));

        System.out.println("Ingestion listening on :" + port + " datalake=" + datalake + " mq=" + mq + " origin=" + origin);
    }

    private static Path detectModuleRoot(Class<?> anchor) {
        try {
            URI uri = anchor.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path p = Paths.get(uri);
            if (Files.isRegularFile(p)) p = p.getParent();
            String name = p.getFileName() != null ? p.getFileName().toString() : "";
            if (name.equals("classes") || name.equals("test-classes")) p = p.getParent();
            name = p.getFileName() != null ? p.getFileName().toString() : "";
            if (name.equals("target") || name.equals("build")) p = p.getParent();
            return p.toAbsolutePath().normalize();
        } catch (Exception e) {
            return Paths.get(".").toAbsolutePath().normalize();
        }
    }
}
