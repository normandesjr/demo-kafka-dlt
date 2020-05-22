package com.hibicode.kafka.controller;

import com.hibicode.kafka.consumer.model.RechargeRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private KafkaTemplate<String, RechargeRequest> kafkaTemplate;

    @GetMapping
    public void test() {
        RechargeRequest request = new RechargeRequest();
        request.setAccount("ohhhhh!");
        kafkaTemplate.send("test1", "key123", request);
    }

}
