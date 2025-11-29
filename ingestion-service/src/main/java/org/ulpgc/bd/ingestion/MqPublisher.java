package org.ulpgc.bd.ingestion;

import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MqPublisher implements AutoCloseable {
    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;
    private static final Gson G = new Gson();

    public MqPublisher(String brokerUrl, String queueName) throws JMSException {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = f.createConnection();
        this.connection.start();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = session.createQueue(queueName);
        this.producer = session.createProducer(dest);
        this.producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    }

    public void publish(Object obj) throws JMSException {
        String json = G.toJson(obj);
        TextMessage msg = session.createTextMessage(json);
        producer.send(msg);
    }

    @Override
    public void close() {
        try { producer.close(); } catch(Exception ignored){}
        try { session.close(); } catch(Exception ignored){}
        try { connection.close(); } catch(Exception ignored){}
    }
}
