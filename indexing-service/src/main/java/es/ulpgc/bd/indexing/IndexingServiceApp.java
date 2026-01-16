package es.ulpgc.bd.indexing;

import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bd.indexing.mq.MqConsumer;
import es.ulpgc.bd.indexing.service.IndexingService;
import io.javalin.Javalin;

import java.net.InetAddress;
import java.util.*;

public class IndexingServiceApp {

    public static void main(String[] args) throws Exception {
        final Map<String, String> a = parseArgs(args);

        final int port = Integer.parseInt(a.getOrDefault("port", "7002"));
        final String mq = a.getOrDefault("mq", "tcp://localhost:61616");

        final String hzClusterTmp = first(a, "hzCluster", "hz.cluster", "hz.clusterName", "hz.cluster-name", "hz");
        final String hzCluster = (hzClusterTmp != null) ? hzClusterTmp : "bd-hz";

        // IMPORTANT: default should be "auto" to match PDF hints (multicast)
        final String hzMembersTmp = first(a, "hzMembers", "hz.members", "hzMembersCsv");
        final String hzMembers = (hzMembersTmp != null) ? hzMembersTmp : "auto";

        final int hzPort = Integer.parseInt(a.getOrDefault("hzPort", a.getOrDefault("hz.port", "5701")));
        final String hzInterface = first(a, "hzInterface", "hz.interface");

        // ingestion base url for debug endpoints (/index/update, /index/rebuild)
        final String ingestion = first(a, "ingestion", "ingestionBase", "ingestion.baseUrl", "ingestionBaseUrl");

        final String ingestQueue = a.getOrDefault("ingestQueue", "ingestion.ingested");
        final boolean mqEnabled = a.getOrDefault("mqIndexingEnabled", "true").equalsIgnoreCase("true");

        final HazelcastInstance hz = HazelcastBoot.startMember(hzCluster, hzMembers, hzPort, hzInterface);
        final IndexingService service = new IndexingService(hz);

        final Javalin app = Javalin.create();

        // ---------- Status endpoints ----------
        app.get("/status", ctx -> {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("service", "indexing");
            s.put("port", port);
            s.put("mq", mq);
            s.put("hzCluster", hzCluster);
            s.put("hzMembers", hzMembers);
            s.put("hzPort", hzPort);
            s.put("hzInterface", hzInterface);
            s.put("ingestion", ingestion);
            s.put("ingestQueue", ingestQueue);
            s.put("mqEnabled", mqEnabled);
            s.putAll(service.stats());
            ctx.json(s);
        });

        app.get("/index/status", ctx -> ctx.json(service.stats()));

        app.get("/hz/members", ctx -> ctx.json(
                hz.getCluster().getMembers().stream().map(Object::toString).toList()
        ));

        // ---------- Indexing endpoints ----------
        // POST /index/update/{bookId}?origin=http://ingestion:7001
        // or: ?sources=http://a:7001,http://b:7001
        app.post("/index/update/{bookId}", ctx -> {
            final int bookId = Integer.parseInt(ctx.pathParam("bookId"));

            String sourcesCsv = ctx.queryParam("sources");
            if (sourcesCsv != null && !sourcesCsv.isBlank()) {
                List<String> src = splitCsv(sourcesCsv);
                ctx.json(service.update(bookId, src));
                return;
            }

            final String origin = ctx.queryParam("origin");
            final String base = (origin != null && !origin.isBlank()) ? origin : ingestion;

            ctx.json(service.update(bookId, base));
        });

        // POST /index/rebuild?origin=http://ingestion:7001
        app.post("/index/rebuild", ctx -> {
            final String origin = ctx.queryParam("origin");
            final String base = (origin != null && !origin.isBlank()) ? origin : ingestion;
            ctx.json(service.rebuild(base));
        });

        app.start(port);

        // ---------- MQ consumer ----------
        if (mqEnabled) {
            MqConsumer consumer = new MqConsumer(mq, ingestQueue, ingestion, service);
            consumer.startAsync();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { app.stop(); } catch (Exception ignored) {}
            try { hz.shutdown(); } catch (Exception ignored) {}
        }));

        System.out.println("Indexing listening on :" + port);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (String s : args) {
            if (!s.startsWith("--")) continue;
            String x = s.substring(2);
            int eq = x.indexOf('=');
            if (eq >= 0) {
                m.put(x.substring(0, eq).trim(), x.substring(eq + 1).trim());
            } else {
                m.put(x.trim(), "true");
            }
        }
        return m;
    }

    private static String first(Map<String, String> m, String... keys) {
        for (String k : keys) {
            String v = m.get(k);
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static List<String> splitCsv(String csv) {
        List<String> out = new ArrayList<>();
        for (String s : csv.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    @SuppressWarnings("unused")
    private static String localIpGuess() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
