package org.taoge.pulsar;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "pulsar")
@Data
public class PulsarParameter {
    private String brokerUrl = "pulsar://localhost:6650";
    private String tenant = "public";
    private String namespace = "default";
    private String subscriptionName = "subscriptionName";
    private String token; //可选
    private String receiverQueueSize; //可选
    private List<String> topics; //必输
}
