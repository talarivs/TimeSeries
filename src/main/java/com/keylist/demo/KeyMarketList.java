package com.keylist.demo;

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
@RequestMapping("/api/marketdata")
public class KeyMarketList {

    @PostMapping("/run")
    public String runJob(@RequestBody String inputJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode input = mapper.readTree(inputJson);

            String validationError = validateInput(input);
            if (validationError != null) {
                return "Invalid input: " + validationError;
            }

            Properties props = new Properties();
            props.load(KeyMarketList.class.getClassLoader().getResourceAsStream("JSONtoDB1_config.properties"));

            try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
                 Statement stmt = conn.createStatement()) {
                stmt.execute(props.getProperty("sql.create.table1"));
                stmt.execute(props.getProperty("sql.create.index1"));
            }

            List<String> bucketAttributes = new ArrayList<>();
            input.get("timeBucketKey").forEach(n -> bucketAttributes.add(n.asText()));

            List<String> timeBuckets = new ArrayList<>();
            input.get("timeBucket").forEach(n -> timeBuckets.add(n.asText()));

            List<String> timeBucketInputs = new ArrayList<>();
            input.get("timeBucketInput").forEach(n -> timeBucketInputs.add(n.asText()));

            Map<String, Object> attributeList = buildAttributes(input.get("attributes"));

            String dateStr;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            if (input.has("date")) {
                dateStr = input.get("date").asText();
            } else if (input.has("year") && input.has("time_bucket")) {
                String q = input.get("time_bucket").asText().toUpperCase();
                String year = input.get("year").asText();
                Map<String, String> qMap = Map.of("Q1", "01", "Q2", "04", "Q3", "07", "Q4", "10");
                dateStr = year + "-" + qMap.getOrDefault(q, "01") + "-01";
            } else if (input.has("year") && input.has("month")) {
                String month = input.get("month").asText();
                String year = input.get("year").asText();
                dateStr = year + "-" + (month.length() == 1 ? "0" + month : month) + "-01";
            } else if (input.has("year")) {
                dateStr = input.get("year").asText() + "-01-01";
            } else {
                dateStr = "2024-01-01";
            }

            Date date = sdf.parse(dateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int month = cal.get(Calendar.MONTH) + 1;
            int year = cal.get(Calendar.YEAR);
            int quarterly = (month - 1) / 3 + 1;
            int half = (month - 1) / 6 + 1;
            int financialYear = (month >= 4) ? year + 1 : year;

            Map<String, String> timeBucketMap = new LinkedHashMap<>();
            timeBucketMap.put("monthly", String.valueOf(month));
            timeBucketMap.put("quarterly", "Q" + quarterly);
            timeBucketMap.put("halfyearly", "H" + half);
            timeBucketMap.put("annually", String.valueOf(year));
            timeBucketMap.put("financialyear", "FY" + financialYear);

            for (int i = 0; i < timeBuckets.size(); i++) {
                String bucketType = timeBuckets.get(i).toLowerCase();
                String bucketValue = timeBucketMap.get(bucketType);
                String timeBucketInput = (i < timeBucketInputs.size()) ? timeBucketInputs.get(i) : "default_input";


                StringBuilder valueBuilder = new StringBuilder();
                List<String> rawParts = new ArrayList<>();

                for (String attr : bucketAttributes) {
                    if (!input.has(attr)) return "Missing value for required attribute: " + attr;
                    String val = input.get(attr).asText();
                    valueBuilder.append(val);
                    rawParts.add(val);
                }

                valueBuilder.append(bucketValue).append(year);
                rawParts.add(bucketValue);
                rawParts.add(String.valueOf(year));

                String key = String.join("_", bucketAttributes) + "_" + bucketType + "_year";
                Map<String, String> metaMap = new HashMap<>();
                metaMap.put(key, valueBuilder.toString());

                String metadataJson = mapper.writeValueAsString(metaMap);
                String attributesJson = mapper.writeValueAsString(attributeList);
                String hashKey = sha256Hex(String.join(":", rawParts));

                try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
                     PreparedStatement ps = conn.prepareStatement(props.getProperty("sql.insert1"))) {
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
    //validation part
    private String validateInput(JsonNode input) {
        List<String> requiredArrays = List.of("timeBucketKey", "timeBucket");
        List<String> requiredFields = List.of("entity", "entityType", "attributes");

        for (String field : requiredFields) {
            if (!input.has(field)) {
                return "Missing required field: " + field;
            }
        }

        for (String arrayField : requiredArrays) {
            if (!input.has(arrayField) || !input.get(arrayField).isArray() || input.get(arrayField).isEmpty()) {
                return "Missing or empty array field: " + arrayField;
            }
        }

        // Check attribute structure
        if (!input.get("attributes").isArray()) {
            return "'attributes' must be an array";
        }

        for (JsonNode attr : input.get("attributes")) {
            if (!attr.has("attributeName") || !attr.has("type")) {
                return "Each attribute must have 'attributeName' and 'type'";
            }
        }

        return null; // everything valid
    }
}
