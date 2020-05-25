package com.hibicode.kafka.service;

import com.hibicode.kafka.model.dto.FormRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class KafkaStartSimulation {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStartSimulation.class);

    @Async
    public void start(FormRequest formRequest) {
        logger.info("Form requested to simulate: " + formRequest);
    }

}
