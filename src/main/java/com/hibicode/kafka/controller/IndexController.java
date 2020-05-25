package com.hibicode.kafka.controller;

import com.hibicode.kafka.model.dto.ErrorType;
import com.hibicode.kafka.model.dto.FormRequest;
import com.hibicode.kafka.model.dto.RetryableStopAt;
import com.hibicode.kafka.service.KafkaStartSimulation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class IndexController {

    private static final Logger logger = LoggerFactory.getLogger(IndexController.class);

    @Autowired
    private KafkaStartSimulation kafkaStartSimulation;

    @GetMapping("/")
    public ModelAndView home() {
        var modelAndView = new ModelAndView("/index");
        modelAndView.addObject("formRequest", new FormRequest(ErrorType.NONE, RetryableStopAt.DLT));
        modelAndView.addObject("allErrorTypes", ErrorType.values());
        modelAndView.addObject("allStopPossibilities", RetryableStopAt.values());
        return modelAndView;
    }

    @PostMapping("/start")
    public ModelAndView start(FormRequest formRequest) {
        kafkaStartSimulation.start(formRequest);
        logger.info("Simulation started");

        var modelAndView = new ModelAndView("/asyncStarted");
        modelAndView.addObject("formRequest", formRequest);
        return modelAndView;
    }

}
