package com.hibicode.kafka.consumer;

import com.hibicode.kafka.exception.NotRetryableException;
import com.hibicode.kafka.exception.RetryableException;
import com.hibicode.kafka.model.dto.FormRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private void process(ConsumerRecord<String, FormRequest> consumerRecord, String receivedTopic) {
        var formRequest = consumerRecord.value();

        log.info("received topic - {}", receivedTopic);

        if (!shouldStopThrowError(receivedTopic, formRequest)) {
            throwSomeException(formRequest);
        }
    }

    private void throwSomeException(FormRequest formRequest) {
        switch (formRequest.getErrorType()) {
            case RETRYABLE:
                throw new RetryableException("Retryable exception");
            case NOT_RETRYABLE:
                throw new NotRetryableException("Not retryable exception");
        }
    }

    private boolean shouldStopThrowError(String receivedTopic, FormRequest formRequest) {
        var retryIndex = receivedTopic.replaceAll("[^\\d]", "");
        return formRequest.getRetryableStopAt().getIndex().equals(retryIndex);
    }

    @KafkaListener(
            id = "main-topic-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "mainKafkaListenerContainerFactory")
    public void process(@Payload ConsumerRecord<String, FormRequest> consumerRecord,
                        Acknowledgment ack,
                        @Header(name = RECEIVED_TOPIC) String receivedTopic) {
        try {
            log.info("Start main consumer");
            process(consumerRecord, receivedTopic);
        } finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(
            id = "retry-kafka-listener",
            topicPattern = "${app.kafka.dlt.retry.topics.pattern}",
            containerFactory = "retryKafkaListenerContainerFactory",
            properties = {
                    "fetch.min.bytes=${app.kafka.dlt.retry.min.bytes}",
                    "fetch.max.wait.ms=${app.kafka.dlt.retry.max.wait.ms}"
            }
    )
    public void retry(@Payload ConsumerRecord<String, FormRequest> consumerRecord,
                      Acknowledgment ack,
                      @Header(name = RECEIVED_TOPIC) String receivedTopic) {
        try {
            log.info("Start retry");
            process(consumerRecord, receivedTopic);
        } finally {
            ack.acknowledge();
        }
    }

}
