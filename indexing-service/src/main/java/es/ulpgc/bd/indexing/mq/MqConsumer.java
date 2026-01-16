package es.ulpgc.bd.indexing.mq;

import com.google.gson.Gson;
import es.ulpgc.bd.indexing.service.IndexingService;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class MqConsumer {

    private final Gson gson = new Gson();

    private final String brokerUrl;
    private final String queueName;
    private final String defaultOrigin;
    private final IndexingService indexing;

    public MqConsumer(String brokerUrl, String queueName, String defaultOrigin, IndexingService indexing) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
        this.defaultOrigin = defaultOrigin;
        this.indexing = indexing;
    }

    public void startAsync() {
        Thread t = new Thread(this::runForever, "mq-consumer-" + queueName);
        t.setDaemon(true);
        t.start();
    }

    private void runForever() {
        while (true) {
            try {
                consumeLoop();
            } catch (Exception e) {
                System.out.println("[MQ] Consumer crashed: " + e.getMessage());
                e.printStackTrace();
                sleep(2000);
            }
        }
    }

    private void consumeLoop() throws Exception {
        Connection connection = null;
        Session session = null;

        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(true, Session.SESSION_TRANSACTED);

            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);

            System.out.println("[MQ] Listening queue=" + queueName + " broker=" + brokerUrl);

            while (true) {
                Message msg = consumer.receive(1000);
                if (msg == null) continue;

                try {
                    handleMessage(msg);
                    session.commit();
                } catch (Exception ex) {
                    System.out.println("[MQ] Processing failed -> rollback: " + ex.getMessage());
                    ex.printStackTrace();
                    try { session.rollback(); } catch (Exception ignored) {}
                    sleep(500);
                }
            }

        } finally {
            try { if (session != null) session.close(); } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }
    }

    private void handleMessage(Message msg) throws Exception {
        DocumentEvent ev = parseEvent(msg);
        if (ev == null || ev.bookId <= 0) {
            System.out.println("[MQ] Bad/empty event, msg=" + msg);
            return;
        }

        List<String> sources = buildSources(ev);

        System.out.println("[MQ] document.ingested bookId=" + ev.bookId + " sources=" + sources);

        var out = indexing.update(ev.bookId, sources);

        if (out != null && "error".equalsIgnoreCase(String.valueOf(out.get("status")))) {
            throw new RuntimeException("indexing.update returned error: " + out);
        }

        System.out.println("[MQ] indexed bookId=" + ev.bookId);
    }

    private List<String> buildSources(DocumentEvent ev) {
        LinkedHashSet<String> s = new LinkedHashSet<>();

        if (ev.sources != null) {
            for (String x : ev.sources) {
                if (x == null) continue;
                String t = x.trim();
                if (t.isEmpty()) continue;
                if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
                s.add(t);
            }
        }

        if (ev.origin != null && !ev.origin.isBlank()) {
            String t = ev.origin.trim();
            if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
            s.add(t);
        }

        if (defaultOrigin != null && !defaultOrigin.isBlank()) {
            String t = defaultOrigin.trim();
            if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
            s.add(t);
        }

        return new ArrayList<>(s);
    }

    private DocumentEvent parseEvent(Message msg) throws Exception {
        if (msg instanceof TextMessage tm) {
            String payload = tm.getText();
            return gson.fromJson(payload, DocumentEvent.class);
        }

        if (msg instanceof MapMessage mm) {
            int bookId = 0;
            try { bookId = mm.getInt("book_id"); } catch (Exception ignored) {}
            if (bookId <= 0) {
                try { bookId = mm.getInt("bookId"); } catch (Exception ignored) {}
            }
            String origin = null;
            try { origin = mm.getString("origin"); } catch (Exception ignored) {}

            return new DocumentEvent(bookId, origin);
        }

        System.out.println("[MQ] Unsupported message type: " + msg.getClass().getName());
        return null;
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (Exception ignored) {}
    }
}
