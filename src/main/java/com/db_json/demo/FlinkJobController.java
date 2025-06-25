package com.db_json.demo;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/flink")
public class FlinkJobController {

    @PostMapping("/run")
    public String runFlinkJob(@RequestBody FlinkInput input) {
        try {
            JSONtoDB1.runFlinkJob(input.getProductId(), input.getRegion(), input.getDate(), input.getYear());
            return "Flink job executed successfully!";
        } catch (Exception e) {
            e.printStackTrace();
            return "Flink job failed: " + e.getMessage();
        }
    }
}