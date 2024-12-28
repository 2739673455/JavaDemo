package com.atguigu.edupublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.edupublisher.mapper")
public class EduPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(EduPublisherApplication.class, args);
    }
}
