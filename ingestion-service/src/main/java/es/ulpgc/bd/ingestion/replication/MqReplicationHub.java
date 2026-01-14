package es.ulpgc.bd.ingestion.replication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MqReplicationHub implements AutoCloseable {

    public interface LocalFiles {
        boolean has(String date, String hour, int bookId, String shaHeader, String shaBody, String shaMeta);
        void store(String date, String hour, int bookId, String header, String body, String metaJson,
                   String shaHeader, String shaBody, String shaMeta) throws Exception;
    }

    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();

    private final String mqUrl;
    private final String origin;
    private final LocalFiles local;
    private final HttpClient http;

    private Connection connection;
    private Session session;

    private Destination replTopic;
    private Destination peersTopic;

    private MessageProducer replProducer;
    private MessageProducer peersProducer;

    private MessageConsumer replConsumer;
    private MessageConsumer peersConsumer;

    private final Set<String> peers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public MqReplicationHub(String mqUrl, String origin, LocalFiles local) {
        this.mqUrl = mqUrl;
        this.origin = origin;
        this.local = local;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(4))
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public void start() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(mqUrl);
        factory.setConnectResponseTimeout(6000);

        connection = factory.createConnection();
        connection.setClientID("ingestion_" + stableId(origin));
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        replTopic = session.createTopic("bd.ingestion.replication");
        peersTopic = session.createTopic("bd.ingestion.peers");

        replProducer = session.createProducer(replTopic);
        replProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        peersProducer = session.createProducer(peersTopic);
        peersProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        replConsumer = session.createConsumer(replTopic);
        replConsumer.setMessageListener(new ReplicationListener());

        peersConsumer = session.createConsumer(peersTopic);
        peersConsumer.setMessageListener(new PeersListener());

        connection.start();

        announceHello();

        scheduler.schedule(this::syncFromPeersSafe, 2, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::syncFromPeersSafe, 15, 15, TimeUnit.SECONDS);
    }

    public void publishIngested(ReplicationEvent ev) throws Exception {
        if (ev == null) return;
        ev.origin = origin;
        if (ev.ts == null || ev.ts.isBlank()) ev.ts = Instant.now().toString();
        String json = G.toJson(ev);

        TextMessage msg = session.createTextMessage(json);
        msg.setStringProperty("origin", origin);
        msg.setStringProperty("type", "INGESTED");
        replProducer.send(msg);
    }

    private void announceHello() {
        try {
            PeerMessage pm = new PeerMessage();
            pm.type = "HELLO";
            pm.origin = origin;
            pm.ts = Instant.now().toString();
            TextMessage msg = session.createTextMessage(G.toJson(pm));
            msg.setStringProperty("origin", origin);
            msg.setStringProperty("type", pm.type);
            peersProducer.send(msg);
        } catch (Exception ignored) {}
    }

    private void announceHelloAck() {
        try {
            PeerMessage pm = new PeerMessage();
            pm.type = "HELLO_ACK";
            pm.origin = origin;
            pm.ts = Instant.now().toString();
            TextMessage msg = session.createTextMessage(G.toJson(pm));
            msg.setStringProperty("origin", origin);
            msg.setStringProperty("type", pm.type);
            peersProducer.send(msg);
        } catch (Exception ignored) {}
    }

    private void syncFromPeersSafe() {
        try {
            syncFromPeers();
        } catch (Exception ignored) {}
    }

    private void syncFromPeers() throws Exception {
        Set<String> snapshot = new HashSet<>(peers);
        snapshot.remove(origin);
        if (snapshot.isEmpty()) return;

        for (String peer : snapshot) {
            ManifestEntry[] manifest = fetchManifest(peer);
            if (manifest == null || manifest.length == 0) continue;

            for (ManifestEntry me : manifest) {
                if (me == null) continue;
                if (me.date == null || me.hour == null) continue;

                boolean ok = local.has(me.date, me.hour, me.bookId, me.sha256Header, me.sha256Body, me.sha256Meta);
                if (ok) continue;

                String header = fetchFile(peer, me.bookId, "header", me.date, me.hour);
                String body = fetchFile(peer, me.bookId, "body", me.date, me.hour);
                String meta = fetchFile(peer, me.bookId, "meta", me.date, me.hour);

                if (header == null) header = "";
                if (body == null) body = "";

                local.store(me.date, me.hour, me.bookId, header, body, meta,
                        me.sha256Header, me.sha256Body, me.sha256Meta);
            }
        }
    }

    private ManifestEntry[] fetchManifest(String peerOrigin) {
        try {
            String url = peerOrigin + "/ingest/manifest";
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(6))
                    .GET()
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return null;
            String body = res.body();
            if (body == null || body.isBlank()) return null;
            return G.fromJson(body, ManifestEntry[].class);
        } catch (Exception e) {
            return null;
        }
    }

    private String fetchFile(String peerOrigin, int bookId, String kind, String date, String hour) {
        try {
            String url = peerOrigin + "/ingest/file/" + bookId + "/" + kind
                    + "?date=" + enc(date) + "&hour=" + enc(hour);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) return null;
            return res.body();
        } catch (Exception e) {
            return null;
        }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static String stableId(String origin) {
        String x = origin == null ? "" : origin.trim().toLowerCase();
        x = x.replace("http://", "").replace("https://", "");
        x = x.replaceAll("[^a-z0-9]+", "_");
        if (x.length() > 60) x = x.substring(0, 60);
        if (x.isBlank()) x = "node";
        return x;
    }

    private class ReplicationListener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                if (!(message instanceof TextMessage tm)) return;
                String json = tm.getText();
                if (json == null || json.isBlank()) return;

                ReplicationEvent ev = G.fromJson(json, ReplicationEvent.class);
                if (ev == null) return;
                if (ev.origin == null || ev.origin.isBlank()) return;
                if (Objects.equals(ev.origin, origin)) return;
                if (ev.date == null || ev.hour == null) return;

                peers.add(ev.origin);

                boolean ok = local.has(ev.date, ev.hour, ev.bookId, ev.sha256Header, ev.sha256Body, ev.sha256Meta);
                if (ok) return;

                String header = fetchFile(ev.origin, ev.bookId, "header", ev.date, ev.hour);
                String body = fetchFile(ev.origin, ev.bookId, "body", ev.date, ev.hour);
                String meta = fetchFile(ev.origin, ev.bookId, "meta", ev.date, ev.hour);

                if (header == null) header = "";
                if (body == null) body = "";

                local.store(ev.date, ev.hour, ev.bookId, header, body, meta,
                        ev.sha256Header, ev.sha256Body, ev.sha256Meta);

            } catch (Exception ignored) {}
        }
    }

    private class PeersListener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                if (!(message instanceof TextMessage tm)) return;
                String json = tm.getText();
                if (json == null || json.isBlank()) return;

                PeerMessage pm = G.fromJson(json, PeerMessage.class);
                if (pm == null || pm.origin == null || pm.origin.isBlank()) return;
                if (Objects.equals(pm.origin, origin)) return;

                peers.add(pm.origin);

                if ("HELLO".equalsIgnoreCase(pm.type)) {
                    announceHelloAck();
                }
            } catch (Exception ignored) {}
        }
    }

    private static class PeerMessage {
        String type;
        String origin;
        String ts;
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdownNow();
        safeClose(peersConsumer);
        safeClose(replConsumer);
        safeClose(peersProducer);
        safeClose(replProducer);
        safeClose(session);
        safeClose(connection);
    }

    private static void safeClose(Object o) {
        if (o == null) return;
        try {
            if (o instanceof MessageConsumer c) c.close();
            else if (o instanceof MessageProducer p) p.close();
            else if (o instanceof Session s) s.close();
            else if (o instanceof Connection c) c.close();
        } catch (JMSException ignored) {}
    }
}
