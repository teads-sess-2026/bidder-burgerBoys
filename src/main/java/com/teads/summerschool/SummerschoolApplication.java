package com.teads.summerschool;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SummerschoolApplication {

	public static void main(String[] args) {
		SpringApplication.run(SummerschoolApplication.class, args);
	}

}
