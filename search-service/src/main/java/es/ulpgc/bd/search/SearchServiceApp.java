package es.ulpgc.bd.search;

import com.hazelcast.config.*;
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

        final String hzCluster = first(a, "hzCluster", "hz.cluster", "hz.clusterName", "hz.cluster-name", "hz");
        final String clusterName = (hzCluster != null) ? hzCluster : "bd-hz";

        final String hzMembersTmp = first(a, "hzMembers", "hz.members", "hzMembersCsv");
        final String hzMembers = (hzMembersTmp != null) ? hzMembersTmp : "auto";

        final int hzPort = Integer.parseInt(a.getOrDefault("hzPort", a.getOrDefault("hz.port", "5701")));
        final String hzInterface = first(a, "hzInterface", "hz.interface");

        Config cfg = new Config();
        cfg.setClusterName(clusterName);

        cfg.addMapConfig(new MapConfig("docs").setBackupCount(2));
        cfg.addMapConfig(new MapConfig("docTerms").setBackupCount(2));

        MultiMapConfig mm = new MultiMapConfig("inverted-index");
        mm.setBackupCount(2);
        mm.setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        cfg.addMultiMapConfig(mm);

        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(hzPort);
        net.setPortAutoIncrement(true);

        if (hzInterface != null && !hzInterface.isBlank()) {
            net.setPublicAddress(hzInterface.trim() + ":" + hzPort);
        }

        JoinConfig join = net.getJoin();
        TcpIpConfig tcp = join.getTcpIpConfig();
        MulticastConfig mc = join.getMulticastConfig();

        if (hzMembers.isBlank()
                || "auto".equalsIgnoreCase(hzMembers)
                || "multicast".equalsIgnoreCase(hzMembers)) {

            tcp.setEnabled(false);
            mc.setEnabled(true);

        } else {
            mc.setEnabled(false);
            tcp.setEnabled(true);

            for (String m : hzMembers.split(",")) {
                String mmbr = m.trim();
                if (mmbr.isEmpty()) continue;
                if (mmbr.contains(":")) tcp.addMember(mmbr);
                else tcp.addMember(mmbr + ":" + hzPort);
            }
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        SearchService service = new SearchService(hz, clusterName, hzMembers, port);

        Javalin app = Javalin.create().start(port);
        new SearchHttpApi(service).register(app);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { app.stop(); } catch (Exception ignored) {}
            try { hz.shutdown(); } catch (Exception ignored) {}
        }));

        System.out.println("Search listening on :" + port + " hzCluster=" + clusterName + " hzMembers=" + hzMembers);
    }

    private static String first(Map<String, String> m, String... keys) {
        for (String k : keys) {
            String v = m.get(k);
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }
}
