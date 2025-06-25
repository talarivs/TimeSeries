package com.db_json.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

@RestController
@RequestMapping("/api/flink")
public class FlinkJobController {

    @PostMapping("/run")
    public String runFlinkJob(@RequestBody String inputJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode input = mapper.readTree(inputJson);

            // Load DB config
            Properties props = new Properties();
            props.load(FlinkJobController.class.getClassLoader().getResourceAsStream("JSONtoDB1_config.properties"));

            // Ensure table
            try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
                 Statement stmt = conn.createStatement()) {
                stmt.execute(props.getProperty("sql.create.table"));
                stmt.execute(props.getProperty("sql.create.index"));
            }

            List<String> bucketAttributes = new ArrayList<>();
            input.get("timeBucketAttributes").forEach(n -> bucketAttributes.add(n.asText()));

            List<String> timeBuckets = new ArrayList<>();
            input.get("timeBucket").forEach(n -> timeBuckets.add(n.asText()));

            List<String> timeBucketInputs = new ArrayList<>();
            input.get("timeBucketInput").forEach(n -> timeBucketInputs.add(n.asText()));

            Map<String, Object> attributeList = buildAttributes(input.get("attributes"));

            String dateStr = input.has("date") ? input.get("date").asText() : "2024-01-01";
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int month = cal.get(Calendar.MONTH) + 1;
            int year = cal.get(Calendar.YEAR);
            int quarter = (month - 1) / 3 + 1;
            int half = (month - 1) / 6 + 1;
            int financialYear = (month >= 4) ? year + 1 : year;

            Map<String, String> timeBucketMap = new LinkedHashMap<>();
            timeBucketMap.put("month", String.valueOf(month));
            timeBucketMap.put("quarter", "Q" + quarter);
            timeBucketMap.put("half", "H" + half);
            timeBucketMap.put("year", "A" + year);
            timeBucketMap.put("financialYear", "FY" + financialYear);

            for (int i = 0; i < timeBuckets.size(); i++) {
                String bucketType = timeBuckets.get(i);
                String bucketValue = timeBucketMap.get(bucketType);
                String timeBucketInput = timeBucketInputs.get(i);

                StringBuilder valueBuilder = new StringBuilder();
                List<String> rawParts = new ArrayList<>();

                for (String attr : bucketAttributes) {
                    String val = input.get(attr).asText();
                    valueBuilder.append(val);
                    rawParts.add(val);
                }

                valueBuilder.append(bucketValue).append(year);
                rawParts.add(bucketValue);
                rawParts.add(String.valueOf(year));

                // Build dynamic key
                String key = String.join("_", bucketAttributes) + "_" + bucketType + "_year";
                Map<String, String> metaMap = new HashMap<>();
                metaMap.put(key, valueBuilder.toString());

                String metadataJson = mapper.writeValueAsString(metaMap);
                String attributesJson = mapper.writeValueAsString(attributeList);
                String hashKey = sha256Hex(String.join(":", rawParts));

                try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
                     PreparedStatement ps = conn.prepareStatement(props.getProperty("sql.insert"))) {
                    ps.setObject(1, metadataJson, java.sql.Types.OTHER);
                    ps.setString(2, hashKey);
                    ps.setObject(3, attributesJson, java.sql.Types.OTHER);
                    ps.executeUpdate();
                }
            }
            return "Inserted successfully";

        } catch (Exception e) {
            e.printStackTrace();
            return "Failed: " + e.getMessage();
        }
    }

    private Map<String, Object> buildAttributes(JsonNode attributesNode) {
        Map<String, Object> result = new HashMap<>();
        for (JsonNode attr : attributesNode) {
            String name = attr.get("attributeName").asText();
            String type = attr.get("type").asText();
            switch (type) {
                case "string":
                    result.put(name, "");
                    break;
                case "array":
                    List<Map<String, Object>> arrayList = new ArrayList<>();
                    JsonNode arrayFields = attr.get("attributes").get(0);
                    Map<String, Object> innerArrayMap = new HashMap<>();
                    arrayFields.fieldNames().forEachRemaining(f -> innerArrayMap.put(f, ""));
                    arrayList.add(innerArrayMap);
                    result.put(name, arrayList);
                    break;
                case "object":
                    Map<String, Object> innerObj = new HashMap<>();
                    JsonNode objFields = attr.get("attributes");
                    objFields.fieldNames().forEachRemaining(f -> innerObj.put(f, ""));
                    result.put(name, innerObj);
                    break;
            }
        }
        return result;
    }
}
