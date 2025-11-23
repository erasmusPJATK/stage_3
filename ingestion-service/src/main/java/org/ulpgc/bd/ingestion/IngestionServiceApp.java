package org.ulpgc.bd.ingestion;

import io.javalin.Javalin;
import io.javalin.json.JavalinGson;
import org.ulpgc.bd.ingestion.api.IngestionHttpApi;
import org.ulpgc.bd.ingestion.io.HttpDownloader;
import org.ulpgc.bd.ingestion.parser.GutenbergMetaExtractor;
import org.ulpgc.bd.ingestion.parser.GutenbergSplitter;
import org.ulpgc.bd.ingestion.service.IngestionService;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IngestionServiceApp {

    public static void main(String[] args) {
        int port = 7001;
        Path moduleRoot = detectModuleRoot(IngestionServiceApp.class);
        Path datalake = moduleRoot.resolve("datalake").toAbsolutePath().normalize();
        String parserVersion = "gutenberg-heuristics-8";

        HttpDownloader downloader = new HttpDownloader("IngestionService/1.0 (+mailto:adrian.budzich101@alu.ulpgc.es)", 6000, 10000);
        GutenbergSplitter splitter = new GutenbergSplitter();
        GutenbergMetaExtractor extractor = new GutenbergMetaExtractor();
        IngestionService service = new IngestionService(datalake, parserVersion, downloader, splitter, extractor);

        Javalin app = Javalin.create(cfg -> cfg.jsonMapper(new JavalinGson())).start(port);
        IngestionHttpApi.register(app, service);
        System.out.println("Ingestion listening on :" + port + " datalake=" + datalake);
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
