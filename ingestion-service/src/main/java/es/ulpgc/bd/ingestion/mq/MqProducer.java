package es.ulpgc.bd.ingestion.mq;

import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class MqProducer {

    private final Gson gson = new Gson();

    private final String brokerUrl;
    private final String queueName;

    public MqProducer(String brokerUrl, String queueName) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    public void publish(int bookId, String origin) throws Exception {
        Connection connection = null;
        Session session = null;

        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);

            MessageProducer producer = session.createProducer(destination);

            DocumentEvent ev = new DocumentEvent(bookId, origin);
            String payload = gson.toJson(ev);

            TextMessage msg = session.createTextMessage(payload);
            producer.send(msg);

        } finally {
            try { if (session != null) session.close(); } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }
    }
}
