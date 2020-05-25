package com.hibicode.kafka.service;

import com.hibicode.kafka.model.dto.FormRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class KafkaStartSimulation {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStartSimulation.class);

    @Value("${app.kafka.topic}")
    private String mainTopicName;

    @Autowired
    private KafkaTemplate<String, FormRequest> kafkaTemplate;

    @Async
    public void start(FormRequest formRequest) {
        logger.info("Form requested to simulate: " + formRequest);
        kafkaTemplate.send(mainTopicName, formRequest);
    }

}
