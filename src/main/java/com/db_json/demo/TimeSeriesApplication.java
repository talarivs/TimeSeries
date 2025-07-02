package com.db_json.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class TimeSeriesApplication {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesApplication.class);

    public static void main(String[] args) {
        logger.info("Starting TimeSeriesApplication...");
        try {
            SpringApplication.run(TimeSeriesApplication.class, args);
            logger.info("TimeSeriesApplication started successfully.");
        } catch (Exception e) {
            logger.error("Application failed to start: {}", e.getMessage(), e);
        }
    }
}
