package org.ulpgc.bd.indexing;

import com.google.gson.Gson;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.Map;
import java.util.function.Consumer;

public class MqConsumer implements AutoCloseable {
    private final Connection connection;
    private final Session session;
    private final MessageConsumer consumer;
    private final Gson G = new Gson();

    public MqConsumer(String brokerUrl, String queueName) throws JMSException {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = f.createConnection();
        this.connection.start();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = session.createQueue(queueName);
        this.consumer = session.createConsumer(dest);
    }

    public void listen(Consumer<Map<String,Object>> handler) throws JMSException {
        consumer.setMessageListener(m -> {
            try {
                if(m instanceof TextMessage){
                    String s = ((TextMessage)m).getText();
                    Map<String,Object> obj = G.fromJson(s, Map.class);
                    handler.accept(obj);
                }
            } catch (Exception ignored){}
        });
    }

    @Override
    public void close() {
        try { consumer.close(); } catch(Exception ignored){}
        try { session.close(); } catch(Exception ignored){}
        try { connection.close(); } catch(Exception ignored){}
    }
}
