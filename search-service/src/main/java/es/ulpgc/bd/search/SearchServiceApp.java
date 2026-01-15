package es.ulpgc.bd.search;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bd.search.api.SearchHttpApi;
import es.ulpgc.bd.search.service.SearchService;
import io.javalin.Javalin;

import java.util.Map;

public class SearchServiceApp {

    public static void main(String[] args) {
        Map<String, String> a = Args.parse(args);

        final int port = Integer.parseInt(a.getOrDefault("port", "7003"));

        final String hzCluster = a.getOrDefault("hzCluster", a.getOrDefault("hz.cluster", "bd-hz"));
        final String hzMembers = a.getOrDefault("hzMembers", a.getOrDefault("hz.members", "127.0.0.1"));
        final int hzPort = Integer.parseInt(a.getOrDefault("hzPort", a.getOrDefault("hz.port", "5701")));

        // ✅ Hazelcast CLIENT (nie MEMBER) -> nie ma split-brain
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName(hzCluster);

        ClientNetworkConfig net = cfg.getNetworkConfig();
        net.setConnectionTimeout(5000);

        for (String m : hzMembers.split(",")) {
            String mm = m.trim();
            if (mm.isEmpty()) continue;
            if (mm.contains(":")) net.addAddress(mm);
            else net.addAddress(mm + ":" + hzPort);
        }

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(cfg);

        // ✅ tu przekazujemy PORT HTTP (7003), nie hzPort
        SearchService service = new SearchService(hz, hzCluster, hzMembers, port);

        Javalin app = Javalin.create().start(port);
        new SearchHttpApi(service).register(app);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { app.stop(); } catch (Exception ignored) {}
            try { hz.shutdown(); } catch (Exception ignored) {}
        }));

        System.out.println("Search listening on :" + port + " hzCluster=" + hzCluster + " (CLIENT)");
    }
}
