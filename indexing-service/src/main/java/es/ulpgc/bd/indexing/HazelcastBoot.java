package es.ulpgc.bd.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastBoot {

    public static HazelcastInstance startMember(String clusterName,
                                                String membersCsv,
                                                int hzPort,
                                                String hzInterface) {

        Config cfg = new Config();
        cfg.setClusterName(clusterName);

        // ---- Map configs (replication / fault tolerance) ----
        cfg.addMapConfig(new MapConfig("docs").setBackupCount(1));
        cfg.addMapConfig(new MapConfig("docTerms").setBackupCount(1));
        cfg.addMapConfig(new MapConfig("inverted").setBackupCount(1));

        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(hzPort);
        net.setPortAutoIncrement(true);

        if (hzInterface != null && !hzInterface.isBlank()) {
            InterfacesConfig ifc = net.getInterfaces();
            ifc.setEnabled(true);
            ifc.addInterface(hzInterface);
            net.setPublicAddress(hzInterface + ":" + hzPort);
        }

        JoinConfig join = net.getJoin();
        join.getMulticastConfig().setEnabled(false);

        TcpIpConfig tcp = join.getTcpIpConfig();
        tcp.setEnabled(true);

        if (membersCsv != null && !membersCsv.isBlank()) {
            for (String m : membersCsv.split(",")) {
                String mm = m.trim();
                if (mm.isEmpty()) continue;
                if (mm.contains(":")) tcp.addMember(mm);
                else tcp.addMember(mm + ":" + hzPort);
            }
        }

        return Hazelcast.newHazelcastInstance(cfg);
    }
}
