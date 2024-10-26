package org.taoge.pulsar;

import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PulsarConsumer {
    private static Map<String, Consumer<String>> consumerMap = new ConcurrentHashMap<>();

    @Autowired
    private PulsarParameter parameter;

    @Autowired
    private PulsarClient client;

    @Autowired
    private PulsarConsumerHandler handler;

    @PostConstruct
    public void initConsumer() throws PulsarClientException {
        String topicPrefix = PulsarConstant.TOPIC_PROTOCOL+parameter.getTenant()+"/"+parameter.getNamespace()+"/";
        for(String topic : parameter.getTopics()){
            Consumer<String> producer = client
                    .newConsumer(Schema.STRING)
                    .topic(topicPrefix + topic)
                    .subscriptionName(parameter.getSubscriptionName())
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
            consumerMap.put(topic, producer);
        }

        //按照topic拉起多个消费者，每个消费者一个独立线程
        initReceivers();
    }

    private void  initReceivers() {
        for(Consumer<String> consumer : consumerMap.values()){
            new Thread(()->receiveMsg(consumer)).start();
        }
    }

    private void receiveMsg(Consumer<String> consumer){
        while (true){
            Message<String> msg = null;
            try {
                msg = consumer.receive();
                if (msg != null){
                    //类似于SPI，需要大家实现规约接口，进行注入
                    //大家自己的实现类，用switch即可
                    handler.receive(consumer.getTopic(), msg.getValue());
                }
            }catch (PulsarClientException e){
                throw new RuntimeException(e);
            }
        }
    }

    @PreDestroy
    public void close() throws PulsarClientException {
        for(Consumer<String> consumer : consumerMap.values()){
            consumer.close();
        }
    }
}
