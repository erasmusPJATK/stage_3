package es.ulpgc.bd.indexing;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastBoot {

    public static HazelcastInstance startMember(String clusterName,
                                                String membersCsv,
                                                int hzPort,
                                                String hzInterface) {

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

        if (membersCsv == null || membersCsv.isBlank() || "auto".equalsIgnoreCase(membersCsv) || "multicast".equalsIgnoreCase(membersCsv)) {
            tcp.setEnabled(false);
            mc.setEnabled(true);
        } else {
            mc.setEnabled(false);
            tcp.setEnabled(true);

            for (String m : membersCsv.split(",")) {
                String mmbr = m.trim();
                if (mmbr.isEmpty()) continue;
                if (mmbr.contains(":")) tcp.addMember(mmbr);
                else tcp.addMember(mmbr + ":" + hzPort);
            }
        }

        return Hazelcast.newHazelcastInstance(cfg);
    }
}
