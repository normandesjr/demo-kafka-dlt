package com.hibicode.kafka.config;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Optional;
import java.util.function.BiFunction;

import static org.springframework.kafka.listener.ContainerProperties.AckMode;
import static org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_TOPIC;

@Configuration
public class RetryKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(RetryKafkaConfig.class);

    static final FixedBackOff NONE_RETRY = new FixedBackOff(0, 0);

    private static final int ANY_PARTITION = -1;

    private String dltTopic = "test1-dlt";
    private String retryTopicsPattern = "test1-retry-[0-9]+";
    private String retryFirstTopic = "test1-retry-1";

    private int retryTopicsCount = 3;

    private Optional<String> originTopic(Headers headers) {
        return Optional.ofNullable(headers.lastHeader(DLT_ORIGINAL_TOPIC))
                        .map(Header::value)
                        .map(bytes -> new String(bytes));
    }

    @Bean
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> retryResolver() {
        return (consumerRecord, exception) -> {

            Optional<String> origem = originTopic(consumerRecord.headers())
                            .filter(t -> t.matches(retryTopicsPattern))
                            .or(() -> Optional.of(retryFirstTopic));

            log.info("Origin topic {}", origem.get());

            String destino =
                    origem
                            .filter(topico -> topico.matches(retryTopicsPattern))
                            .map(t -> t.substring(t.lastIndexOf("-")))
                            .map(n -> n.split("-"))
                            .map(n -> n[1])
                            .map(Integer::parseInt)
                            .filter(n -> n < retryTopicsCount)
                            .map(n -> origem.get().substring(0, origem.get().lastIndexOf("-")) + "-" + (n + 1))
                            .orElse(dltTopic);

            log.info("Tópico destino do registro {}", destino);

            return new TopicPartition(destino, ANY_PARTITION);
        };
    }

    @Bean
    public SeekToCurrentErrorHandler retryErrorHandler(
            @Qualifier("retryResolver")
                    BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolver,
            KafkaTemplate<?, ?> template) {

        var recoverer = new DeadLetterPublishingRecoverer(template, resolver);
        // There's no need for retry local here
        var handler = new SeekToCurrentErrorHandler(recoverer, NONE_RETRY);

        return handler;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, GenericRecord>>
    retryKafkaListenerContainerFactory(
            @Qualifier("retryErrorHandler")
                    SeekToCurrentErrorHandler errorHandler,
            KafkaProperties properties,
            ConsumerFactory<String, GenericRecord> factory){

        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> listener =
                new ConcurrentKafkaListenerContainerFactory<>();

        listener.setConsumerFactory(factory);
        listener.setErrorHandler(errorHandler);

//        // Falhar, caso os tópicos não existam?
//        listener.getContainerProperties()
//                .setMissingTopicsFatal(missingTopicsFatal);

        listener.getContainerProperties().setAckMode(AckMode.MANUAL);
        listener.getContainerProperties().setSyncCommits(Boolean.TRUE);

        return listener;
    }

}
