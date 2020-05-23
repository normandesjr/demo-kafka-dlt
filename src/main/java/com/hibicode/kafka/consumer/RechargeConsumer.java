package com.hibicode.kafka.consumer;

import com.hibicode.kafka.consumer.model.RechargeRequest;
import com.hibicode.kafka.exception.NotRetryableException;
import com.hibicode.kafka.exception.RetryableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class RechargeConsumer {

    private static final Logger log = LoggerFactory.getLogger(RechargeConsumer.class);

    @KafkaListener(topics = "test1", containerFactory = "kafkaJsonListenerContainerFactory")
    public void process(@Payload ConsumerRecord<String, RechargeRequest> consumerRecord, Acknowledgment ack) {
        try {
//            throw new NotRetryableException();
            throw new RetryableException();
//            log.info("Account: " + consumerRecord.value().getAccount() + " key: " + consumerRecord.key());
        } finally {
            ack.acknowledge();
        }
    }

}
