package org.ulpgc.bd.ingestion;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

public class MqPublisher implements AutoCloseable {
    private final Connection connection;
    private final Session session;

    public MqPublisher(String brokerUrl) throws Exception {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = f.createConnection();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.connection.start();
    }

    public void publishTopic(String topicName, String json) throws Exception {
        Topic topic = session.createTopic(topicName);
        MessageProducer p = session.createProducer(topic);
        TextMessage msg = session.createTextMessage(json == null ? "" : json);
        p.send(msg);
        p.close();
    }

    @Override
    public void close() throws Exception {
        try { session.close(); } catch (Exception ignored) {}
        try { connection.close(); } catch (Exception ignored) {}
    }
}
