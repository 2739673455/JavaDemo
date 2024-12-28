package com.atguigu.dga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class DgaApplication {

    public static void main(String[] args) {
        SpringApplication.run(DgaApplication.class, args);
    }
}
