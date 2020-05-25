package com.hibicode.kafka.controller;

import com.hibicode.kafka.controller.dto.ErrorType;
import com.hibicode.kafka.controller.dto.FormRequest;
import com.hibicode.kafka.controller.dto.RetryableStopAt;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class IndexController {

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
        // TODO: Start async message to Kafka

        var modelAndView = new ModelAndView("/asyncStarted");
        modelAndView.addObject("formRequest", formRequest);
        return modelAndView;
    }

}
