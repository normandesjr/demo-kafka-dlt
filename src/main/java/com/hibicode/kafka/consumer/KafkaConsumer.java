package com.hibicode.kafka.consumer;

import com.hibicode.kafka.exception.NotRetryableException;
import com.hibicode.kafka.exception.RetryableException;
import com.hibicode.kafka.model.dto.FormRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private void process(ConsumerRecord<String, FormRequest> consumerRecord) {
        var formRequest = consumerRecord.value();

        switch (formRequest.getErrorType()) {
            case RETRYABLE:
                throw new RetryableException("Retryable exception");
            case NOT_RETRYABLE:
                throw new NotRetryableException("Not retryable exception");
        }
    }

    @KafkaListener(
            id = "main-topic-listener",
            topics = "${app.kafka.topic}",
            containerFactory = "mainKafkaListenerContainerFactory")
    public void process(@Payload ConsumerRecord<String, FormRequest> consumerRecord, Acknowledgment ack) {
        try {
            log.info("Start main consumer");
            process(consumerRecord);
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
    public void retry(@Payload ConsumerRecord<String, FormRequest> consumerRecord, Acknowledgment ack) throws Exception {
        try {
            log.info("Start retry");
            process(consumerRecord);
        } finally {
            ack.acknowledge();
        }
    }

}
