package es.ulpgc.bd.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
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

        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(port);
        net.setPortAutoIncrement(true);

        if (hzInterface != null && !hzInterface.isBlank()) {
            InterfacesConfig ifc = net.getInterfaces();
            ifc.setEnabled(true);
            ifc.addInterface(hzInterface.trim());
        }

        JoinConfig join = net.getJoin();
        join.getMulticastConfig().setEnabled(false);

        TcpIpConfig tcp = join.getTcpIpConfig();
        tcp.setEnabled(true);

        List<String> members = Arrays.stream(membersCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        for (String m : members) tcp.addMember(m);

        return Hazelcast.newHazelcastInstance(cfg);
    }
}
