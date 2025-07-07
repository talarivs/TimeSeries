package com.db_json.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TimeSeriesService implements CommandLineRunner {

    @Autowired
    private TimeSeriesController timeSeriesService;

    public static void main(String[] args) {
        SpringApplication.run(TimeSeriesService.class, args);
    }

    @Override
    public void run(String... args) {
        String filePath = "C:/Users/talari.vs_simadvisor/Desktop/TimeSeries/Timestamp/src/main/resources/TimeSeries.json";
        String entityName = "product_metrics";

        String result = timeSeriesService.processJsonFile(filePath, entityName);
        System.out.println("=== Final Output ===");
        System.out.println(result);
    }
}
