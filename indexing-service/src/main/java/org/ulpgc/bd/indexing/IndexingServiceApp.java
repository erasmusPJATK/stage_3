package org.ulpgc.bd.indexing;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import io.javalin.Javalin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IndexingServiceApp {
    public static void main(String[] args) throws Exception {
        Args a = new Args(args);
        int port = a.getInt("port",7002);
        String broker = a.get("mq","tcp://localhost:61616");
        String hzMembers = a.get("hz","127.0.0.1:5701");
        String cluster = a.get("cluster","bd-stage3");
        int hzPort = a.getInt("hzPort",5701);

        HazelcastInstance hz = HazelcastBoot.start(cluster, hzMembers, hzPort);
        MultiMap<String,Integer> inverted = hz.getMultiMap("inverted-index");
        IMap<Integer, DocMeta> docs = hz.getMap("docs");
        IMap<Integer, Map<String,Integer>> docTerms = hz.getMap("doc-terms");

        HttpClient http = HttpClient.newHttpClient();
        MqConsumer consumer = new MqConsumer(broker,"books.ingested");
        consumer.listen(msg -> {
            try {
                Double d = (Double) msg.get("bookId");
                int id = d.intValue();
                String origin = Objects.toString(msg.get("origin"), "http://localhost:7001");
                String h = fetch(http, origin+"/ingest/file/"+id+"/header");
                String b = fetch(http, origin+"/ingest/file/"+id+"/body");
                DocMeta meta = TextUtil.parseHeader(h, id);
                List<String> toks = TextUtil.tokenize(b);
                Map<String,Integer> tf = new HashMap<>();
                for(String t : toks) tf.merge(t,1,Integer::sum);
                for(String t : tf.keySet()) inverted.put(t, id);
                docs.put(id, meta);
                docTerms.put(id, tf);
            } catch (Exception ignored) {}
        });

        Javalin app = Javalin.create(c -> c.http.defaultContentType="application/json").start(port);
        app.get("/status", ctx -> {
            ctx.json(Map.of(
                    "service","indexing",
                    "port",port,
                    "hzMembers",hzMembers,
                    "docs", docs.size(),
                    "terms", inverted.keySet().size()
            ));
        });
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
    }

    private static String fetch(HttpClient http, String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET().build();
        HttpResponse<byte[]> res = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if(res.statusCode()!=200) throw new IllegalStateException("fetch failed "+url);
        return new String(res.body(), StandardCharsets.UTF_8);
    }
}
