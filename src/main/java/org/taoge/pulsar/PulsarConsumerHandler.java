package org.taoge.pulsar;

public interface PulsarConsumerHandler {
    void receive(String topic, String msg);
}
