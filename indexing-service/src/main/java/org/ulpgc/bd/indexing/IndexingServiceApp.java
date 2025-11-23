package org.ulpgc.bd.indexing;

import io.javalin.Javalin;
import io.javalin.json.JavalinGson;
import org.ulpgc.bd.indexing.api.IndexingHttpApi;
import org.ulpgc.bd.indexing.service.IndexingService;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IndexingServiceApp {
    public static void main(String[] args) {
        int port = 7002;
        Path moduleRoot = detectModuleRoot(IndexingServiceApp.class);
        Path repoRoot = moduleRoot.getParent() != null ? moduleRoot.getParent() : moduleRoot;
        Path datalake = repoRoot.resolve("ingestion-service").resolve("datalake").toAbsolutePath().normalize();
        Path datamarts = moduleRoot.resolve("datamarts").toAbsolutePath().normalize();
        String indexerVersion = "indexer-1.0";

        IndexingService service = new IndexingService(datalake, datamarts, indexerVersion);

        Javalin app = Javalin.create(cfg -> cfg.jsonMapper(new JavalinGson())).start(port);
        IndexingHttpApi.register(app, service);
        System.out.println("Indexing listening on :" + port + " datalake=" + datalake + " datamarts=" + datamarts);
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
