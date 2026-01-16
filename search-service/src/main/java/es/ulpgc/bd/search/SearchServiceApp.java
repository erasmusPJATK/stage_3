package es.ulpgc.bd.search;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
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
        final String hzInterface = a.getOrDefault("hzInterface", a.getOrDefault("hz.interface", ""));

        Config cfg = new Config();
        cfg.setClusterName(hzCluster);

        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(hzPort);
        net.setPortAutoIncrement(true);

        if (!hzInterface.isBlank()) {
            InterfacesConfig ifc = net.getInterfaces();
            ifc.setEnabled(true);
            ifc.addInterface(hzInterface);
            net.setPublicAddress(hzInterface + ":" + hzPort);
        }

        JoinConfig join = net.getJoin();
        join.getMulticastConfig().setEnabled(false);

        TcpIpConfig tcp = join.getTcpIpConfig();
        tcp.setEnabled(true);

        for (String m : hzMembers.split(",")) {
            String mm = m.trim();
            if (mm.isEmpty()) continue;
            if (mm.contains(":")) tcp.addMember(mm);
            else tcp.addMember(mm + ":" + hzPort);
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        SearchService service = new SearchService(hz, hzCluster, hzMembers, port);

        Javalin app = Javalin.create().start(port);
        new SearchHttpApi(service).register(app);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { app.stop(); } catch (Exception ignored) {}
            try { hz.shutdown(); } catch (Exception ignored) {}
        }));

        System.out.println("Search listening on :" + port + " hzCluster=" + hzCluster);
    }
}
