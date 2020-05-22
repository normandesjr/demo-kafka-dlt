package com.hibicode.kafka.config;

import com.hibicode.kafka.consumer.model.RechargeRequest;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.JsonMessageConverter;

@Configuration
public class KafkaConfig {

    @Bean
    public ConsumerFactory<String, RechargeRequest> consumerFactory(KafkaProperties properties) {
        return new DefaultKafkaConsumerFactory<String, RechargeRequest>(properties.buildConsumerProperties());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RechargeRequest> kafkaJsonListenerContainerFactory(
            ConsumerFactory<String, RechargeRequest> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, RechargeRequest>();
        factory.setConsumerFactory(consumerFactory);
        factory.setMessageConverter(new JsonMessageConverter());
        return factory;
    }

}
