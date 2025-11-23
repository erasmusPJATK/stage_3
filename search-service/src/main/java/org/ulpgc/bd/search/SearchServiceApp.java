package org.ulpgc.bd.search;

import io.javalin.Javalin;
import io.javalin.json.JavalinGson;
import org.ulpgc.bd.search.api.SearchHttpApi;
import org.ulpgc.bd.search.service.SearchService;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SearchServiceApp {

    public static void main(String[] args) {
        int port = 7003;
        Path moduleRoot = detectModuleRoot(SearchServiceApp.class);
        Path repoRoot = moduleRoot.getParent() != null ? moduleRoot.getParent() : moduleRoot;
        Path datamarts = repoRoot.resolve("indexing-service").resolve("datamarts").toAbsolutePath().normalize();

        SearchService service = new SearchService(datamarts);

        Javalin app = Javalin.create(cfg -> {
            cfg.jsonMapper(new JavalinGson());
            cfg.http.defaultContentType = "application/json";
        }).start(port);

        SearchHttpApi.register(app, service);
        System.out.println("Search Service running on http://localhost:" + port + " datamarts=" + datamarts);
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
