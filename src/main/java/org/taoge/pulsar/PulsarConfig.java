package org.taoge.pulsar;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.List;

@Configuration
public class PulsarConfig {
    @Autowired
    private PulsarParameter parameter;

    @Bean
    public PulsarClient pulsarClient() throws PulsarClientException {
        String tokenStr = parameter.getToken();
        String brokerUrl = parameter.getBrokerUrl();
        if(StringUtils.hasText(tokenStr)){
            Authentication token = new AuthenticationToken(tokenStr);
            return PulsarClient.builder()
                    .serviceUrl(brokerUrl)
                    .authentication(token)
                    .build();
        }else{
            return PulsarClient.builder()
                    .serviceUrl(brokerUrl)
                    .build();
        }
    }

    @PostConstruct
    public void parameterCheck(){
        List<String> topics = parameter.getTopics();
        if(CollectionUtils.isEmpty(topics)){
            throw new IllegalStateException("Pulsar topics is not configured correctly");
        }
    }
}
