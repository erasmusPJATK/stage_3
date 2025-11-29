package org.ulpgc.bd.search;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;

public class HazelcastBoot {
    public static HazelcastInstance start(String clusterName, String membersCsv, int port){
        Config c = new Config();
        c.setClusterName(clusterName);
        c.getNetworkConfig().setPort(port).setPortAutoIncrement(true);
        JoinConfig j = c.getNetworkConfig().getJoin();
        j.getMulticastConfig().setEnabled(false);
        j.getTcpIpConfig().setEnabled(true).setMembers(Arrays.asList(membersCsv.split("\\s*,\\s*")));
        NearCacheConfig ncDocs = new NearCacheConfig("docs");
        NearCacheConfig ncTf = new NearCacheConfig("doc-terms");
        c.getMapConfig("docs").setNearCacheConfig(ncDocs);
        c.getMapConfig("doc-terms").setNearCacheConfig(ncTf);
        return Hazelcast.newHazelcastInstance(c);
    }
}
