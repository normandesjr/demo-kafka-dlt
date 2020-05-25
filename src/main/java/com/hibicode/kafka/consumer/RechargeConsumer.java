package com.hibicode.kafka.consumer;

import com.hibicode.kafka.consumer.model.RechargeRequest;
import com.hibicode.kafka.exception.NotRetryableException;
import com.hibicode.kafka.exception.RetryableException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class RechargeConsumer {

    private static final Logger log = LoggerFactory.getLogger(RechargeConsumer.class);

    @KafkaListener(topics = "test1", containerFactory = "mainKafkaListenerContainerFactory")
    public void process(@Payload ConsumerRecord<String, RechargeRequest> consumerRecord, Acknowledgment ack) {
        try {
            log.info("################ Trying process!");
//            throw new NotRetryableException();
            throw new RetryableException("Error retryable!");
        } finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(
            id = "retry-kafka-listener",
            topicPattern = "test1-retry-[0-9]+",
            containerFactory = "retryKafkaListenerContainerFactory",
            properties = {
                    "fetch.min.bytes=${app.kafka.dlt.retry.min.bytes}",
                    "fetch.max.wait.ms=${app.kafka.dlt.retry.max.wait.ms}"
            }
    )
    public void retry(@Payload ConsumerRecord<String, RechargeRequest> consumerRecord,
                      Acknowledgment ack) throws Exception {
        try {
            log.info("Retry message {}", consumerRecord.value());
            process(consumerRecord); // ideally should be equals as the main listener, here is just for the example
        } finally {
            ack.acknowledge();
        }
    }

    private void process(ConsumerRecord<String, RechargeRequest> consumerRecord) {
//        log.info("Retry successfully");
        throw new RetryableException("No retry");
    }

}
