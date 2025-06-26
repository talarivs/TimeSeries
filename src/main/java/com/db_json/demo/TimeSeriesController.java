package com.db_json.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

@RestController
@RequestMapping("/api")
public class TimeSeriesController {

    @PostMapping("/timeseries/run")
    public String runTimeSeriesJob(@RequestBody String inputJson) {
        return runJob(inputJson, "product_metrics");
    }

    @PostMapping("/customerdata/run")
    public String runCustomerDataJob(@RequestBody String inputJson) {
        return runJob(inputJson, "key_customer_list");
    }

    @PostMapping("/marketdata/run")
    public String runMarketDataJob(@RequestBody String inputJson) {
        return runJob(inputJson, "key_market_list");
    }

    private String runJob(String inputJson, String entity) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode input = mapper.readTree(inputJson);

            String validationError = validateInput(input);
            if (validationError != null) {
                return "Invalid input: " + validationError;
            }

            Properties props = new Properties();
            props.load(TimeSeriesController.class.getClassLoader().getResourceAsStream("JSONtoDB1_config.properties"));

            try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
                 Statement stmt = conn.createStatement()) {
                String tableQuery = String.format(props.getProperty("sql.create.table"), entity);
                String indexQuery = String.format(props.getProperty("sql.create.index"), entity);
                String tableQuery1 = String.format(props.getProperty("sql.create.table1"), entity);
                String indexQuery1 = String.format(props.getProperty("sql.create.index1"), entity);
                String tableQuery2 = String.format(props.getProperty("sql.create.table2"), entity);
                String indexQuery2 = String.format(props.getProperty("sql.create.index2"), entity);
                stmt.execute(tableQuery);
                stmt.execute(indexQuery);
                stmt.execute(tableQuery1);
                stmt.execute(indexQuery1);
                stmt.execute(tableQuery2);
                stmt.execute(indexQuery2);
            }

            List<String> bucketAttributes = new ArrayList<>();
            input.get("timeBucketKey").forEach(n -> bucketAttributes.add(n.asText()));

            List<String> timeBuckets = new ArrayList<>();
            input.get("timeBucket").forEach(n -> timeBuckets.add(n.asText()));

            List<String> timeBucketInputs = new ArrayList<>();
            if (input.has("timeBucketInput")) {
                input.get("timeBucketInput").forEach(n -> timeBucketInputs.add(n.asText()));
            }

            Map<String, Object> attributeList = buildAttributes(input.get("attributes"));
            String dateStr = parseDate(input);
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int month = cal.get(Calendar.MONTH) + 1;
            int year = cal.get(Calendar.YEAR);
            int quarterly = (month - 1) / 3 + 1;
            int half = (month - 1) / 6 + 1;
            int financialYear = (month >= 4) ? year + 1 : year;

            Map<String, String> timeBucketMap = buildTimeBucketMap(month, year, quarterly, half, financialYear);

            for (int i = 0; i < timeBuckets.size(); i++) {
                String bucketType = timeBuckets.get(i).toLowerCase();
                String bucketValue = timeBucketMap.getOrDefault(bucketType, "");
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
                String metadataJson = mapper.writeValueAsString(Map.of(key, valueBuilder.toString()));
                String attributesJson = mapper.writeValueAsString(attributeList);
                String hashInput = String.join(":", rawParts);

                // ✅ INSERT call
                saveToDatabase(entity, metadataJson, hashInput, attributesJson);
            }

            return "Inserted successfully";

        } catch (Exception e) {
            e.printStackTrace();
            return "Failed: " + e.getMessage();
        }
    }

    private Map<String, String> buildTimeBucketMap(int month, int year, int quarterly, int half, int financialYear) {
        Map<String, String> timeBucketMap = new LinkedHashMap<>();
        timeBucketMap.put("monthly", String.valueOf(month));
        timeBucketMap.put("quarterly", "Q" + quarterly);
        timeBucketMap.put("halfyearly", "H" + half);
        timeBucketMap.put("annually", String.valueOf(year));
        timeBucketMap.put("financialyear", "FY" + financialYear);
        return timeBucketMap;
    }

    private void saveToDatabase(String entity, String metadataJson, String hashKey, String attributesJson) {
        try {
            Properties props = new Properties();
            props.load(TimeSeriesController.class.getClassLoader().getResourceAsStream("JSONtoDB1_config.properties"));

            try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"))) {
                String insertQuery = getInsertQueryForEntity(entity, props);
                if (insertQuery == null) {
                    throw new IllegalArgumentException("No insert query found for entity: " + entity);
                }

                try (PreparedStatement ps = conn.prepareStatement(insertQuery)) {
                    ps.setString(1, metadataJson);
                    ps.setString(2, hashKey); // sha256Hex stored in time_bucket_key TEXT
                    ps.setString(3, attributesJson);
                    ps.executeUpdate();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getInsertQueryForEntity(String entity, Properties props) {
        return switch (entity) {
            case "product_metrics" -> props.getProperty("sql.insert");
            case "key_market_list" -> props.getProperty("sql.insert1");
            case "key_customer_list" -> props.getProperty("sql.insert2");
            default -> null;
        };
    }

    private String parseDate(JsonNode input) {
        if (input.has("date")) {
            return input.get("date").asText();
        } else if (input.has("year") && input.has("time_bucket")) {
            String q = input.get("time_bucket").asText().toUpperCase();
            String year = input.get("year").asText();
            Map<String, String> qMap = Map.of("Q1", "01", "Q2", "04", "Q3", "07", "Q4", "10");
            return year + "-" + qMap.getOrDefault(q, "01") + "-01";
        } else if (input.has("year") && input.has("month")) {
            String month = input.get("month").asText();
            String year = input.get("year").asText();
            return year + "-" + (month.length() == 1 ? "0" + month : month) + "-01";
        } else if (input.has("year")) {
            return input.get("year").asText() + "-01-01";
        } else {
            return "2024-01-01";
        }
    }

    private Map<String, Object> buildAttributes(JsonNode attributesNode) {
        Map<String, Object> result = new HashMap<>();
        for (JsonNode attr : attributesNode) {
            String name = attr.get("attributeName").asText();
            String type = attr.get("type").asText();
            switch (type) {
                case "string" -> result.put(name, "");
                case "array" -> {
                    List<Map<String, Object>> arrayList = new ArrayList<>();
                    JsonNode arrayFields = attr.get("attributes").get(0);
                    Map<String, Object> innerArrayMap = new HashMap<>();
                    arrayFields.fieldNames().forEachRemaining(f -> innerArrayMap.put(f, ""));
                    arrayList.add(innerArrayMap);
                    result.put(name, arrayList);
                }
                case "object" -> {
                    Map<String, Object> innerObj = new HashMap<>();
                    JsonNode objFields = attr.get("attributes");
                    objFields.fieldNames().forEachRemaining(f -> innerObj.put(f, ""));
                    result.put(name, innerObj);
                }
            }
        }
        return result;
    }

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

        if (!input.get("attributes").isArray()) {
            return "'attributes' must be an array";
        }

        for (JsonNode attr : input.get("attributes")) {
            if (!attr.has("attributeName") || !attr.has("type")) {
                return "Each attribute must have 'attributeName' and 'type'";
            }
        }

        return null;
    }
}
