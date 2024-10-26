package org.taoge.pulsar;

import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PulsarProducer {

    private static Map<String, Producer<String>> producerMap = new ConcurrentHashMap<>();

    @Autowired
    private PulsarParameter parameter;

    @Autowired
    private PulsarClient client;

    @PostConstruct
    public void initProducer() throws PulsarClientException {
        String topicPrefix = PulsarConstant.TOPIC_PROTOCOL+parameter.getTenant()+"/"+parameter.getNamespace()+"/";
        for(String topic : parameter.getTopics()){
            Producer<String> producer = client
                    .newProducer(Schema.STRING)
                    .topic(topicPrefix + topic)
                    .create();
            producerMap.put(topic, producer);
        }
    }

    public MessageId send(String topic, String msg) throws PulsarClientException {
        if(producerMap.containsKey(topic)){
            return producerMap.get(topic).send(msg);
        }else{
            throw  new PulsarClientException("topic not found");
        }
    }

    public CompletableFuture<MessageId> sendAsync(String topic, String msg) throws PulsarClientException {
        if(producerMap.containsKey(topic)){
            return producerMap.get(topic).sendAsync(msg);
        }else{
            throw  new PulsarClientException("topic not found");
        }
    }

    @PreDestroy
    public void close() throws PulsarClientException {
        for(Producer<String> producer : producerMap.values()){
            producer.close();
        }
    }
}
