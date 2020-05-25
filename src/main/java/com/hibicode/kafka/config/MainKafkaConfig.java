package com.hibicode.kafka.config;

import com.hibicode.kafka.exception.NotRetryableException;
import com.hibicode.kafka.exception.RetryableException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
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
public class MainKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(MainKafkaConfig.class);

    static final FixedBackOff RETRY_3X = new FixedBackOff(0, 2);

    private static final int ANY_PARTITION = -1;
    private static final String NONE_HEADER = "__$$none";

    @Value("${app.kafka.dlt.topic}")
    private String dltTopic;

    @Value("${app.kafka.dlt.retry.topics.pattern}")
    private String retryTopicsPattern;

    @Value("${app.kafka.dlt.retry.topic.first}")
    private String retryFirstTopic;

    @Autowired
    private KafkaProperties properties;

    @Bean
    public ConsumerFactory<?, ?> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties());
    }

    private boolean isRetryable(Exception e) {
        boolean result = false;
        var throwableCase = ExceptionUtils.getRootCause(e);
        log.error(throwableCase.getMessage(), throwableCase);
        // TODO: Use properties to get retryable exception
        result = RetryableException.class.equals(throwableCase.getClass());
        log.info("{} is {}retryable", throwableCase.getClass(), (result ? "" : "not-"));
        return result;
    }

    private Optional<String> originTopic(Headers headers) {
        return Optional.ofNullable(headers.lastHeader(DLT_ORIGINAL_TOPIC))
                        .map(Header::value)
                        .map(bytes -> new String(bytes));
    }

    @Bean
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> mainResolver() {
        return (consumerRecord, exception) -> {
            TopicPartition result = new TopicPartition(dltTopic, ANY_PARTITION);

            if (isRetryable(exception)) {
                Optional<String> origin = originTopic(consumerRecord.headers())
                                .or(() -> Optional.of(NONE_HEADER));

                log.info("main config - origin topic: {}", origin);

                String destinyTopic = origin
                                .filter(topico -> !topico.matches(retryTopicsPattern))
                                .map(t -> retryFirstTopic)
                                .orElse(dltTopic);

                log.info("main config - destiny topic: {}", destinyTopic);

                result = new TopicPartition(destinyTopic, ANY_PARTITION);

            }

            return result;
        };
    }

    @Bean
    public SeekToCurrentErrorHandler mainErrorHandler(
            @Qualifier("mainResolver") BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolver,
            KafkaTemplate<?, ?> template) {

        // If we decide to remove recoverer, the message will not be recovered, only retry
        var recoverer = new DeadLetterPublishingRecoverer(template, resolver);
        var handler = new SeekToCurrentErrorHandler(recoverer, RETRY_3X);

        // TODO: Add using properties
        handler.addNotRetryableException(NotRetryableException.class);

        return handler;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, GenericRecord>>
        mainKafkaListenerContainerFactory(
                @Qualifier("mainErrorHandler") SeekToCurrentErrorHandler errorHandler,
                ConsumerFactory<String, GenericRecord> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, GenericRecord>();
        factory.setConsumerFactory(consumerFactory);
        factory.setErrorHandler(errorHandler);

        factory.getContainerProperties().setMissingTopicsFatal(true);

        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.getContainerProperties().setSyncCommits(Boolean.TRUE);

        return factory;
    }

}
