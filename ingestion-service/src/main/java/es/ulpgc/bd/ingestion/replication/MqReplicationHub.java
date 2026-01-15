package es.ulpgc.bd.ingestion.replication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

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
        void store(String date, String hour, int bookId, String header, String body, String metaJson, String shaHeader, String shaBody, String shaMeta) throws Exception;
    }

    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();

    private static final String TOPIC_HELLO = "bd.ingestion.hello";
    private static final String TOPIC_EVENTS = "bd.ingestion.events";

    private final String brokerUrl;
    private final String origin;
    private final String nodeId;
    private final LocalFiles local;

    private volatile Connection connection;
    private volatile Session session;
    private volatile MessageProducer producerHello;
    private volatile MessageProducer producerEvents;
    private volatile MessageConsumer consumerHello;
    private volatile MessageConsumer consumerEvents;

    private final Map<String, Object> state = new ConcurrentHashMap<>();
    private final Map<String, Long> peers = new ConcurrentHashMap<>();

    public MqReplicationHub(String brokerUrl, String origin, LocalFiles local) {
        this.brokerUrl = brokerUrl;
        this.origin = origin;
        this.local = local;
        this.nodeId = originHost(origin) + "_" + originPort(origin);
        state.put("nodeId", nodeId);
        state.put("origin", origin);
        state.put("mq", brokerUrl);
        state.put("connected", false);
        state.put("peers", new ArrayList<>());
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

            System.out.println("REPL START nodeId=" + nodeId + " origin=" + origin + " mq=" + brokerUrl);
            publishHello();
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

    public Map<String, Object> state() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", state.get("nodeId"));
        m.put("origin", state.get("origin"));
        m.put("mq", state.get("mq"));
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
            System.out.println("REPL TX HELLO origin=" + origin);
        } catch (Exception e) {
            state.put("lastError", e.toString());
            throw new RuntimeException("REPL hello failed: " + e.getMessage(), e);
        }
    }

    private void onHello(Message msg) {
        try {
            if (!(msg instanceof TextMessage)) return;
            String s = ((TextMessage) msg).getText();
            @SuppressWarnings("unchecked")
            Map<String, Object> m = G.fromJson(s, Map.class);
            Object t = m.get("type");
            if (t == null || !"HELLO".equals(t.toString())) return;

            String peerOrigin = str(m.get("origin"));
            if (peerOrigin == null || peerOrigin.isBlank()) return;
            if (peerOrigin.equals(origin)) return;

            peers.put(peerOrigin, System.currentTimeMillis());
            state.put("lastHelloRx", Instant.now().toString());
            System.out.println("REPL RX HELLO from=" + peerOrigin);

            new Thread(() -> {
                try { syncFromPeer(peerOrigin); } catch (Exception ex) { state.put("lastError", ex.toString()); }
            }).start();

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
            if (ev.origin == null || ev.origin.isBlank()) return;
            if (ev.origin.equals(origin)) return;

            state.put("lastEventRx", Instant.now().toString());
            System.out.println("REPL RX EVENT origin=" + ev.origin + " bookId=" + ev.bookId + " date=" + ev.date + " hour=" + ev.hour);

            if (local.has(ev.date, ev.hour, ev.bookId, ev.sha256Header, ev.sha256Body, ev.sha256Meta)) return;

            String header = httpGet(ev.origin + "/ingest/file/" + ev.bookId + "/header?date=" + ev.date + "&hour=" + ev.hour);
            String body = httpGet(ev.origin + "/ingest/file/" + ev.bookId + "/body?date=" + ev.date + "&hour=" + ev.hour);

            String meta = "";
            try {
                meta = httpGet(ev.origin + "/ingest/file/" + ev.bookId + "/meta?date=" + ev.date + "&hour=" + ev.hour);
            } catch (Exception ignored) {}

            local.store(ev.date, ev.hour, ev.bookId, header, body, meta, ev.sha256Header, ev.sha256Body, ev.sha256Meta);
            System.out.println("REPL STORED origin=" + ev.origin + " bookId=" + ev.bookId + " date=" + ev.date + " hour=" + ev.hour);

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
