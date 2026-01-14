package es.ulpgc.bd.ingestion.replication;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MqReplicationHub implements AutoCloseable {

    public interface LocalFiles {
        boolean has(String date, String hour, int bookId, String shaHeader, String shaBody, String shaMeta);
        void store(String date, String hour, int bookId, String header, String body, String metaJson, String shaHeader, String shaBody, String shaMeta) throws Exception;
        List<ManifestEntry> manifest();
    }

    private static final Gson G = new GsonBuilder().disableHtmlEscaping().create();

    private static final String VT_EVENTS = "VirtualTopic.bd.replication.events";
    private static final String VT_HELLO = "VirtualTopic.bd.replication.hello";
    private static final String SUB_EVENTS_PREFIX = "Consumer.%s.VirtualTopic.bd.replication.events";
    private static final String SUB_HELLO_PREFIX = "Consumer.%s.VirtualTopic.bd.replication.hello";

    private final String brokerUrl;
    private final String origin;
    private final String nodeId;
    private final LocalFiles local;

    private Connection connection;
    private Session session;

    private MessageProducer eventsProducer;
    private MessageProducer helloProducer;

    private MessageConsumer eventsConsumer;
    private MessageConsumer helloConsumer;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ExecutorService exec = Executors.newFixedThreadPool(4);

    public MqReplicationHub(String brokerUrl, String origin, LocalFiles local) {
        this.brokerUrl = brokerUrl;
        this.origin = origin;
        this.local = local;
        this.nodeId = sanitizeNodeId(origin);
        if (origin != null && (origin.contains("localhost") || origin.contains("127.0.0.1"))) {
            System.out.println("REPL WARN origin looks local: " + origin);
        }
    }

    public void start() throws JMSException {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = f.createConnection();
        this.connection.start();

        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination eventsTopic = session.createTopic(VT_EVENTS);
        Destination helloTopic = session.createTopic(VT_HELLO);

        this.eventsProducer = session.createProducer(eventsTopic);
        this.eventsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        this.helloProducer = session.createProducer(helloTopic);
        this.helloProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        Destination eventsQueue = session.createQueue(String.format(SUB_EVENTS_PREFIX, nodeId));
        Destination helloQueue = session.createQueue(String.format(SUB_HELLO_PREFIX, nodeId));

        this.eventsConsumer = session.createConsumer(eventsQueue);
        this.helloConsumer = session.createConsumer(helloQueue);

        this.eventsConsumer.setMessageListener(this::onEventsMessage);
        this.helloConsumer.setMessageListener(this::onHelloMessage);

        System.out.println("REPL START nodeId=" + nodeId + " origin=" + origin + " mq=" + brokerUrl);
        publishHello();
    }

    public void publishIngested(ReplicationEvent ev) throws JMSException {
        ev.type = "FILE";
        ev.origin = origin;
        sendJson(eventsProducer, ev);
        System.out.println("REPL TX FILE bookId=" + ev.bookId + " date=" + ev.date + " hour=" + ev.hour + " origin=" + ev.origin);
    }

    public void publishHello() throws JMSException {
        ReplicationEvent ev = new ReplicationEvent();
        ev.type = "HELLO";
        ev.origin = origin;
        sendJson(helloProducer, ev);
        System.out.println("REPL TX HELLO origin=" + ev.origin);
    }

    private void onHelloMessage(Message m) {
        ReplicationEvent ev = readEvent(m);
        if (ev == null) return;
        if (!"HELLO".equals(ev.type)) return;
        if (ev.origin == null) return;
        if (Objects.equals(ev.origin, origin)) return;
        System.out.println("REPL RX HELLO from=" + ev.origin);
        exec.submit(() -> replayTo(ev.origin));
    }

    private void replayTo(String targetOrigin) {
        try {
            List<ManifestEntry> ms = local.manifest();
            for (ManifestEntry me : ms) {
                ReplicationEvent ev = new ReplicationEvent();
                ev.type = "FILE";
                ev.origin = origin;
                ev.target = targetOrigin;
                ev.bookId = me.bookId;
                ev.date = me.date;
                ev.hour = me.hour;
                ev.sha256Header = me.sha256Header;
                ev.sha256Body = me.sha256Body;
                ev.sha256Meta = me.sha256Meta;
                ev.parserVersion = me.parserVersion;
                sendJson(eventsProducer, ev);
            }
            System.out.println("REPL REPLAY to=" + targetOrigin + " count=" + ms.size());
        } catch (Exception e) {
            System.out.println("REPL REPLAY FAIL to=" + targetOrigin + " err=" + e.getMessage());
        }
    }

    private void onEventsMessage(Message m) {
        ReplicationEvent ev = readEvent(m);
        if (ev == null) return;
        if (!"FILE".equals(ev.type)) return;
        if (ev.origin == null) return;
        if (Objects.equals(ev.origin, origin)) return;
        if (ev.target != null && !Objects.equals(ev.target, origin)) return;

        String date = ev.date;
        String hour = ev.hour;
        int bookId = ev.bookId;

        if (date == null || hour == null || bookId <= 0) return;

        System.out.println("REPL RX FILE from=" + ev.origin + " bookId=" + bookId + " date=" + date + " hour=" + hour + " target=" + ev.target);

        boolean ok = local.has(date, hour, bookId, ev.sha256Header, ev.sha256Body, ev.sha256Meta);
        if (ok) {
            System.out.println("REPL SKIP have bookId=" + bookId + " date=" + date + " hour=" + hour);
            return;
        }

        exec.submit(() -> {
            try {
                String header = httpGetFile(ev.origin, bookId, "header", date, hour);
                String body = httpGetFile(ev.origin, bookId, "body", date, hour);
                String meta = httpGetFile(ev.origin, bookId, "meta", date, hour);
                local.store(date, hour, bookId, header, body, meta, ev.sha256Header, ev.sha256Body, ev.sha256Meta);
                System.out.println("REPL STORED from=" + ev.origin + " bookId=" + bookId + " date=" + date + " hour=" + hour);
            } catch (Exception e) {
                System.out.println("REPL DL FAIL from=" + ev.origin + " bookId=" + bookId + " err=" + e.getMessage());
            }
        });
    }

    private String httpGetFile(String fromOrigin, int bookId, String kind, String date, String hour) throws Exception {
        String base = fromOrigin.endsWith("/") ? fromOrigin.substring(0, fromOrigin.length() - 1) : fromOrigin;
        String url = base + "/ingest/file/" + bookId + "/" + kind + "?date=" + date + "&hour=" + hour;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 200 && resp.statusCode() < 300) return resp.body();
        throw new RuntimeException("HTTP " + resp.statusCode() + " url=" + url);
    }

    private void sendJson(MessageProducer p, ReplicationEvent ev) throws JMSException {
        String s = G.toJson(ev);
        TextMessage tm = session.createTextMessage(s);
        p.send(tm);
    }

    private ReplicationEvent readEvent(Message m) {
        try {
            if (!(m instanceof TextMessage)) return null;
            String s = ((TextMessage) m).getText();
            return G.fromJson(s, ReplicationEvent.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static String sanitizeNodeId(String origin) {
        String x = origin == null ? "node" : origin;
        x = x.replace("http://", "").replace("https://", "");
        x = x.replaceAll("[^A-Za-z0-9_.-]", "_");
        if (x.length() > 180) x = x.substring(0, 180);
        if (x.isBlank()) x = "node";
        return x;
    }

    @Override
    public void close() {
        try { if (helloConsumer != null) helloConsumer.close(); } catch (Exception ignored) {}
        try { if (eventsConsumer != null) eventsConsumer.close(); } catch (Exception ignored) {}
        try { if (helloProducer != null) helloProducer.close(); } catch (Exception ignored) {}
        try { if (eventsProducer != null) eventsProducer.close(); } catch (Exception ignored) {}
        try { if (session != null) session.close(); } catch (Exception ignored) {}
        try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        try { exec.shutdownNow(); } catch (Exception ignored) {}
    }
}
