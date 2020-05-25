package com.hibicode.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class DemoKafkaDltApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoKafkaDltApplication.class, args);
	}

}
