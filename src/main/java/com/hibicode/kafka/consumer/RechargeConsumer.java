package com.hibicode.kafka.consumer;

import com.hibicode.kafka.consumer.model.RechargeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RechargeConsumer {

    private static final Logger log = LoggerFactory.getLogger(RechargeConsumer.class);

    @KafkaListener(topics = "test1", containerFactory = "kafkaJsonListenerContainerFactory")
    public void process(RechargeRequest msg) {
        log.info("Msg: " + msg);
    }

}
