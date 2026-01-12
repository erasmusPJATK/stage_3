package org.ulpgc.bd.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
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
        MultiMapConfig mm = new MultiMapConfig("inverted-index");
        mm.setBackupCount(2).setAsyncBackupCount(0);
        c.addMultiMapConfig(mm);
        MapConfig docs = new MapConfig("docs");
        docs.setBackupCount(2);
        c.addMapConfig(docs);
        MapConfig tfs = new MapConfig("doc-terms");
        tfs.setBackupCount(2);
        c.addMapConfig(tfs);
        return Hazelcast.newHazelcastInstance(c);
    }
}
