package com.hibicode.kafka.config;

import com.hibicode.kafka.consumer.model.RechargeRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.converter.JsonMessageConverter;

import static org.springframework.kafka.listener.ContainerProperties.*;

@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaProperties properties;

    @Bean
    public ConsumerFactory<String, RechargeRequest> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RechargeRequest> kafkaJsonListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, RechargeRequest>();
        factory.setConsumerFactory(consumerFactory());
        factory.setMessageConverter(new JsonMessageConverter());

        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.getContainerProperties().setSyncCommits(Boolean.TRUE);

        return factory;
    }

}
