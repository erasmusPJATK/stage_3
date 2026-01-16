package es.ulpgc.bd.ingestion.replication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MqReplicationHub implements AutoCloseable {

    public interface LocalFiles {
        boolean has(String date, String hour, int bookId, String shaHeader, String shaBody, String shaMeta);
        void store(String date, String hour, int bookId, String header, String body, String metaJson,
                   String shaHeader, String shaBody, String shaMeta) throws Exception;
    }

    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();

    private static final String TOPIC_HELLO = "bd.ingestion.hello";
    private static final String TOPIC_EVENTS = "bd.ingestion.events";

    private final String brokerUrl;
    private final String origin;
    private final String nodeId;
    private final LocalFiles local;

    // Replication factor R (usually 2 or 3)
    private final int replFactor;

    private volatile Connection connection;
    private volatile Session session;
    private volatile MessageProducer producerHello;
    private volatile MessageProducer producerEvents;
    private volatile MessageConsumer consumerHello;
    private volatile MessageConsumer consumerEvents;

    private final Map<String, Object> state = new ConcurrentHashMap<>();
    private final Map<String, Long> peers = new ConcurrentHashMap<>();

    public MqReplicationHub(String brokerUrl, String origin, LocalFiles local, int replFactor) {
        this.brokerUrl = brokerUrl;
        this.origin = normalizeOrigin(origin);
        this.local = local;
        this.replFactor = Math.max(1, replFactor);

        this.nodeId = originHost(this.origin) + "_" + originPort(this.origin);

        state.put("nodeId", nodeId);
        state.put("origin", this.origin);
        state.put("mq", brokerUrl);
        state.put("replicationFactor", this.replFactor);
        state.put("connected", false);
        state.put("lastError", "");
        state.put("lastHelloRx", "");
        state.put("lastEventRx", "");
        state.put("lastSync", "");
    }

    public void start() {
        try {
            ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(brokerUrl);
            connection = f.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic hello = session.createTopic(TOPIC_HELLO);
            Topic events = session.createTopic(TOPIC_EVENTS);

            producerHello = session.createProducer(hello);
            producerHello.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            producerEvents = session.createProducer(events);
            producerEvents.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            consumerHello = session.createConsumer(hello);
            consumerEvents = session.createConsumer(events);

            consumerHello.setMessageListener(this::onHello);
            consumerEvents.setMessageListener(this::onEvent);

            state.put("connected", true);

            System.out.println("REPL START nodeId=" + nodeId + " origin=" + origin + " mq=" + brokerUrl + " R=" + replFactor);

            // announce self
            publishHello();

            // simple heartbeat so late joiners will still discover existing nodes
            Thread hb = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(10_000);
                        publishHello();
                    } catch (Exception ignored) {}
                }
            }, "repl-hello-heartbeat");
            hb.setDaemon(true);
            hb.start();

        } catch (Exception e) {
            state.put("connected", false);
            state.put("lastError", e.toString());
            throw new RuntimeException("REPL MQ connect failed: " + e.getMessage(), e);
        }
    }

    public void publishIngested(ReplicationEvent ev) {
        try {
            String json = G.toJson(ev);
            TextMessage m = session.createTextMessage(json);
            producerEvents.send(m);
            System.out.println("REPL TX EVENT type=INGESTED origin=" + ev.origin + " bookId=" + ev.bookId + " date=" + ev.date + " hour=" + ev.hour);
        } catch (Exception e) {
            state.put("lastError", e.toString());
            throw new RuntimeException("REPL publish failed: " + e.getMessage(), e);
        }
    }

    /**
     * Replica set for a doc: primaryOrigin + (R-1) peers picked deterministically by bookId.
     * This matches the "replication factor R" requirement from the Stage 3 PDF.
     */
    public List<String> replicaSetFor(String primaryOrigin, int bookId) {
        String primary = normalizeOrigin(primaryOrigin);

        List<String> all = knownOriginsSorted();
        if (!all.contains(primary)) {
            all.add(primary);
            Collections.sort(all);
        }

        int r = Math.min(replFactor, all.size());
        if (r <= 1) return List.of(primary);

        // candidates = all origins except primary
        List<String> candidates = new ArrayList<>(all);
        candidates.remove(primary);

        int need = Math.min(r - 1, candidates.size());
        List<String> out = new ArrayList<>();
        out.add(primary);

        if (need <= 0) return out;

        int start = Math.floorMod(Integer.hashCode(bookId), candidates.size());
        for (int i = 0; i < need; i++) {
            out.add(candidates.get((start + i) % candidates.size()));
        }
        return out;
    }

    public Map<String, Object> state() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", state.get("nodeId"));
        m.put("origin", state.get("origin"));
        m.put("mq", state.get("mq"));
        m.put("replicationFactor", state.get("replicationFactor"));
        m.put("connected", state.get("connected"));
        m.put("lastError", state.get("lastError"));
        m.put("lastHelloRx", state.get("lastHelloRx"));
        m.put("lastEventRx", state.get("lastEventRx"));
        m.put("lastSync", state.get("lastSync"));

        List<String> ps = new ArrayList<>(peers.keySet());
        Collections.sort(ps);
        m.put("peers", ps);

        return m;
    }

    private void publishHello() {
        try {
            Map<String, Object> hello = new LinkedHashMap<>();
            hello.put("type", "HELLO");
            hello.put("origin", origin);
            hello.put("nodeId", nodeId);
            hello.put("ts", Instant.now().toString());

            TextMessage m = session.createTextMessage(G.toJson(hello));
            producerHello.send(m);
        } catch (Exception e) {
            state.put("lastError", e.toString());
        }
    }

    private void onHello(Message msg) {
        try {
            if (!(msg instanceof TextMessage)) return;
            String s = ((TextMessage) msg).getText();

            @SuppressWarnings("unchecked")
            Map<String, Object> m = G.fromJson(s, Map.class);

            if (!"HELLO".equals(String.valueOf(m.get("type")))) return;

            String peerOrigin = normalizeOrigin(str(m.get("origin")));
            if (peerOrigin == null || peerOrigin.isBlank()) return;
            if (peerOrigin.equals(origin)) return;

            boolean isNewPeer = (peers.putIfAbsent(peerOrigin, System.currentTimeMillis()) == null);
            peers.put(peerOrigin, System.currentTimeMillis());

            state.put("lastHelloRx", Instant.now().toString());

            // IMPORTANT FIX: handshake, so late joiners discover existing nodes
            if (isNewPeer) {
                publishHello();
                new Thread(() -> {
                    try { syncFromPeer(peerOrigin); } catch (Exception ex) { state.put("lastError", ex.toString()); }
                }, "repl-sync-" + peerOrigin).start();
            }
        } catch (Exception e) {
            state.put("lastError", e.toString());
        }
    }

    private void onEvent(Message msg) {
        try {
            if (!(msg instanceof TextMessage)) return;
            String s = ((TextMessage) msg).getText();

            ReplicationEvent ev = G.fromJson(s, ReplicationEvent.class);
            if (ev == null) return;
            if (!"INGESTED".equals(ev.type)) return;

            ev.origin = normalizeOrigin(ev.origin);
            if (ev.origin == null || ev.origin.isBlank()) return;
            if (ev.origin.equals(origin)) return;

            // store only if this node belongs to the replica set for this document
            if (!replicaSetFor(ev.origin, ev.bookId).contains(origin)) return;

            state.put("lastEventRx", Instant.now().toString());

            if (local.has(ev.date, ev.hour, ev.bookId, ev.sha256Header, ev.sha256Body, ev.sha256Meta)) return;

            String header = httpGet(ev.origin + "/ingest/file/" + ev.bookId + "/header?date=" + ev.date + "&hour=" + ev.hour);
            String body = httpGet(ev.origin + "/ingest/file/" + ev.bookId + "/body?date=" + ev.date + "&hour=" + ev.hour);

            String meta = "";
            try {
                meta = httpGet(ev.origin + "/ingest/file/" + ev.bookId + "/meta?date=" + ev.date + "&hour=" + ev.hour);
            } catch (Exception ignored) {}

            local.store(ev.date, ev.hour, ev.bookId, header, body, meta, ev.sha256Header, ev.sha256Body, ev.sha256Meta);

        } catch (Exception e) {
            state.put("lastError", e.toString());
        }
    }

    private void syncFromPeer(String peerOrigin) throws Exception {
        String json = httpGet(peerOrigin + "/ingest/manifest");
        ManifestEntry[] arr = G.fromJson(json, ManifestEntry[].class);
        if (arr == null) return;

        int copied = 0;
        for (ManifestEntry me : arr) {
            if (me == null) continue;

            // ONLY copy documents for which we are in the replica set (replication factor R)
            String primary = normalizeOrigin(peerOrigin);
            if (!replicaSetFor(primary, me.bookId).contains(origin)) continue;

            if (me.date == null || me.hour == null) continue;

            if (local.has(me.date, me.hour, me.bookId, me.sha256Header, me.sha256Body, me.sha256Meta)) continue;

            String header = httpGet(peerOrigin + "/ingest/file/" + me.bookId + "/header?date=" + me.date + "&hour=" + me.hour);
            String body = httpGet(peerOrigin + "/ingest/file/" + me.bookId + "/body?date=" + me.date + "&hour=" + me.hour);

            String meta = "";
            try {
                meta = httpGet(peerOrigin + "/ingest/file/" + me.bookId + "/meta?date=" + me.date + "&hour=" + me.hour);
            } catch (Exception ignored) {}

            local.store(me.date, me.hour, me.bookId, header, body, meta, me.sha256Header, me.sha256Body, me.sha256Meta);
            copied++;
        }

        state.put("lastSync", Instant.now().toString());
        System.out.println("REPL SYNC from=" + peerOrigin + " copied=" + copied);
    }

    private List<String> knownOriginsSorted() {
        Set<String> s = new HashSet<>();
        s.add(origin);
        s.addAll(peers.keySet());
        List<String> out = new ArrayList<>(s);
        out.removeIf(x -> x == null || x.isBlank());
        Collections.sort(out);
        return out;
    }

    private static String httpGet(String url) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setRequestMethod("GET");
        c.setConnectTimeout(5000);
        c.setReadTimeout(15000);
        int code = c.getResponseCode();
        if (code != 200) throw new RuntimeException("HTTP " + code + " for " + url);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line).append("\n");
            return sb.toString();
        }
    }

    private static String str(Object o) {
        return o == null ? null : o.toString();
    }

    private static String normalizeOrigin(String o) {
        if (o == null) return null;
        String x = o.trim();
        if (x.endsWith("/")) x = x.substring(0, x.length() - 1);
        return x;
    }

    private static String originHost(String origin) {
        try {
            String x = origin.replace("http://", "").replace("https://", "");
            int i = x.indexOf('/');
            if (i >= 0) x = x.substring(0, i);
            int p = x.indexOf(':');
            return p >= 0 ? x.substring(0, p) : x;
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static int originPort(String origin) {
        try {
            String x = origin.replace("http://", "").replace("https://", "");
            int i = x.indexOf('/');
            if (i >= 0) x = x.substring(0, i);
            int p = x.indexOf(':');
            return p >= 0 ? Integer.parseInt(x.substring(p + 1)) : 80;
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public void close() {
        try { if (consumerHello != null) consumerHello.close(); } catch (Exception ignored) {}
        try { if (consumerEvents != null) consumerEvents.close(); } catch (Exception ignored) {}
        try { if (producerHello != null) producerHello.close(); } catch (Exception ignored) {}
        try { if (producerEvents != null) producerEvents.close(); } catch (Exception ignored) {}
        try { if (session != null) session.close(); } catch (Exception ignored) {}
        try { if (connection != null) connection.close(); } catch (Exception ignored) {}
    }
}
