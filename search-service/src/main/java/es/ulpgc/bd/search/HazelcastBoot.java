package es.ulpgc.bd.search;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastBoot {

    public static HazelcastInstance startMember(String clusterName, String membersCsv, int port, String hzInterface) {
        Config cfg = new Config();
        cfg.setClusterName(clusterName);

        int backupCount = 2;
        cfg.addMapConfig(new MapConfig("docs").setBackupCount(backupCount));
        cfg.addMapConfig(new MapConfig("docTerms").setBackupCount(backupCount));
        cfg.addMultiMapConfig(new MultiMapConfig("inverted-index").setBackupCount(backupCount));

        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(port);
        net.setPortAutoIncrement(true);

        if (hzInterface != null && !hzInterface.isBlank()) {
            InterfacesConfig ifc = net.getInterfaces();
            ifc.setEnabled(true);
            ifc.addInterface(hzInterface.trim());
        }

        JoinConfig join = net.getJoin();
        TcpIpConfig tcp = join.getTcpIpConfig();
        MulticastConfig mc = join.getMulticastConfig();

        if (membersCsv == null || membersCsv.isBlank()
                || "auto".equalsIgnoreCase(membersCsv)
                || "multicast".equalsIgnoreCase(membersCsv)) {
            tcp.setEnabled(false);
            mc.setEnabled(true);
        } else {
            mc.setEnabled(false);
            tcp.setEnabled(true);

            List<String> members = Arrays.stream(membersCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            for (String m : members) {
                if (m.contains(":")) tcp.addMember(m);
                else tcp.addMember(m + ":" + port);
            }
        }

        return Hazelcast.newHazelcastInstance(cfg);
    }
}
