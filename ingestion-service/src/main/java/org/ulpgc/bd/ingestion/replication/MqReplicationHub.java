package org.ulpgc.bd.ingestion.replication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.*;

public class MqReplicationHub implements AutoCloseable {

    public interface LocalFiles {
        boolean has(String date, String hour, int bookId, String shaHeader, String shaBody, String shaMeta);
        void store(String date, String hour, int bookId, String header, String body, String metaJson,
                   String shaHeader, String shaBody, String shaMeta) throws Exception;
        List<ManifestEntry> manifest();
    }

    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();
    private static final Type LIST_MANIFEST = new TypeToken<List<ManifestEntry>>() {}.getType();

    private final String brokerUrl;
    private final String origin;
    private final LocalFiles local;
    private final FileDatalakeReplica replica;

    private Connection connection;
    private Session session;
    private MessageProducer eventsProducer;
    private MessageProducer genericProducer;
    private MessageConsumer eventsConsumer;
    private MessageConsumer syncConsumer;

    private Topic eventsTopic;
    private Topic syncTopic;

    public MqReplicationHub(String brokerUrl, String origin, LocalFiles local) {
        this.brokerUrl = brokerUrl;
        this.origin = origin == null ? "" : origin;
        this.local = local;
        this.replica = new FileDatalakeReplica();
    }

    public void start() throws Exception {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = f.createConnection();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        this.eventsTopic = session.createTopic("bd.ingestion.events");
        this.syncTopic = session.createTopic("bd.ingestion.sync");

        this.eventsProducer = session.createProducer(eventsTopic);
        this.genericProducer = session.createProducer(null);

        this.eventsConsumer = session.createConsumer(eventsTopic);
        this.syncConsumer = session.createConsumer(syncTopic);

        this.eventsConsumer.setMessageListener(this::onEventsMessage);
        this.syncConsumer.setMessageListener(this::onSyncMessage);

        this.connection.start();

        requestSyncOnce();
    }

    public void publishIngested(ReplicationEvent ev) throws Exception {
        if (ev == null) return;
        ev.type = "INGESTED";
        ev.origin = this.origin;

        TextMessage msg = session.createTextMessage(G.toJson(ev));
        msg.setStringProperty("type", "INGESTED");
        eventsProducer.send(msg);
    }

    private void onEventsMessage(Message m) {
        try {
            if (!(m instanceof TextMessage tm)) return;
            String json = tm.getText();
            if (json == null || json.isBlank()) return;

            ReplicationEvent ev = G.fromJson(json, ReplicationEvent.class);
            if (ev == null) return;
            if (ev.origin != null && ev.origin.equals(this.origin)) return;

            ManifestEntry me = new ManifestEntry();
            me.bookId = ev.bookId;
            me.date = ev.date;
            me.hour = ev.hour;
            me.sha256Header = ev.sha256Header;
            me.sha256Body = ev.sha256Body;
            me.sha256Meta = ev.sha256Meta;
            me.parserVersion = ev.parserVersion;
            me.origin = ev.origin;

            replica.replicate(me, local);
        } catch (Exception ignored) {}
    }

    private void onSyncMessage(Message m) {
        try {
            if (!(m instanceof TextMessage tm)) return;
            String json = tm.getText();
            if (json == null || json.isBlank()) return;

            Map<?, ?> payload = G.fromJson(json, Map.class);
            Object t = payload.get("type");
            if (t == null) return;

            String type = String.valueOf(t);
            if (!"SYNC_REQUEST".equals(type)) return;

            Destination replyTo = m.getJMSReplyTo();
            if (replyTo == null) return;

            List<ManifestEntry> entries = local.manifest();
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("type", "MANIFEST");
            resp.put("origin", origin);
            resp.put("ts", Instant.now().toString());
            resp.put("entries", entries);

            TextMessage out = session.createTextMessage(G.toJson(resp));
            out.setStringProperty("type", "MANIFEST");
            genericProducer.send(replyTo, out);
        } catch (Exception ignored) {}
    }

    private void requestSyncOnce() {
        try {
            TemporaryQueue reply = session.createTemporaryQueue();
            MessageConsumer replyConsumer = session.createConsumer(reply);

            Map<String, Object> req = new LinkedHashMap<>();
            req.put("type", "SYNC_REQUEST");
            req.put("origin", origin);
            req.put("ts", Instant.now().toString());

            TextMessage msg = session.createTextMessage(G.toJson(req));
            msg.setJMSReplyTo(reply);
            msg.setStringProperty("type", "SYNC_REQUEST");
            genericProducer.send(syncTopic, msg);

            long deadline = System.currentTimeMillis() + 4000;
            while (System.currentTimeMillis() < deadline) {
                Message r = replyConsumer.receive(800);
                if (!(r instanceof TextMessage tm)) continue;

                Map<?, ?> body = G.fromJson(tm.getText(), Map.class);
                Object typ = body.get("type");
                if (typ == null || !"MANIFEST".equals(String.valueOf(typ))) continue;

                String srcOrigin = body.get("origin") == null ? "" : String.valueOf(body.get("origin"));
                if (srcOrigin.equals(origin)) continue;

                Object entObj = body.get("entries");
                String entJson = G.toJson(entObj);
                List<ManifestEntry> entries = G.fromJson(entJson, LIST_MANIFEST);
                if (entries == null) continue;

                for (ManifestEntry e : entries) {
                    if (e == null) continue;
                    if (e.origin == null || e.origin.isBlank()) e.origin = srcOrigin;
                    replica.replicate(e, local);
                }
            }

            try { replyConsumer.close(); } catch (Exception ignored) {}
            try { reply.delete(); } catch (Exception ignored) {}
        } catch (Exception ignored) {}
    }

    @Override
    public void close() throws Exception {
        try { if (eventsConsumer != null) eventsConsumer.close(); } catch (Exception ignored) {}
        try { if (syncConsumer != null) syncConsumer.close(); } catch (Exception ignored) {}
        try { if (eventsProducer != null) eventsProducer.close(); } catch (Exception ignored) {}
        try { if (genericProducer != null) genericProducer.close(); } catch (Exception ignored) {}
        try { if (session != null) session.close(); } catch (Exception ignored) {}
        try { if (connection != null) connection.close(); } catch (Exception ignored) {}
    }
}
