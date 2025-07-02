package com.db_json.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@RequestMapping("/api")
public class TimeSeriesController {
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesController.class);

    @PostMapping("/timeseries/run")
    public String runTimeSeriesJob(@RequestBody String inputJson) {
        logger.info("Entering runTimeSeriesJob for product_metrics");
        String result = runJob(inputJson, "product_metrics");
        logger.info("Exiting runTimeSeriesJob");
        return result;
    }

    @PostMapping("/customerdata/run")
    public String runCustomerDataJob(@RequestBody String inputJson) {
        logger.info("Entering runCustomerDataJob for key_customer_list");
        String result = runJob(inputJson, "key_customer_list");
        logger.info("Exiting runCustomerDataJob");
        return result;
    }

    @PostMapping("/marketdata/run")
    public String runMarketDataJob(@RequestBody String inputJson) {
        logger.info("Entering runMarketDataJob for key_market_list");
        String result = runJob(inputJson, "key_market_list");
        logger.info("Exiting runMarketDataJob");
        return result;
    }

    private String runJob(String inputJson, String entity) {
        logger.info("Entering runJob for entity: {}", entity);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode input = mapper.readTree(inputJson);

            try {
                TimeSeriesInputValidator.validate(input);
            } catch (InvalidInputException e) {
                logger.error("Input validation failed: {}", e.getMessage());
                return "Invalid input: " + e.getMessage();
            }

            List<String> bucketAttributes = new ArrayList<>();
            input.get("timeBucketKey").forEach(n -> bucketAttributes.add(n.asText()));

            List<String> timeBuckets = new ArrayList<>();
            input.get("timeBucket").forEach(n -> timeBuckets.add(n.asText()));

            Map<String, Object> attributeList = buildAttributes(input.get("attributes"));
            String dateStr = parseDate(input);
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
            Timestamp timestamp = new Timestamp(date.getTime());

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int month = cal.get(Calendar.MONTH) + 1;
            int year = cal.get(Calendar.YEAR);
            int quarterly = (month - 1) / 3 + 1;
            int half = (month - 1) / 6 + 1;
            int financialYear = (month >= 4) ? year + 1 : year;

            Map<String, String> timeBucketMap = buildTimeBucketMap(month, year, quarterly, half, financialYear);

            Properties props = new Properties();
            props.load(TimeSeriesController.class.getClassLoader().getResourceAsStream("JSONtoDB1_config.properties"));

            try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
                 Statement stmt = conn.createStatement()) {

                logger.info("Connected to database and preparing to create table/index for {}", entity);

                switch (entity) {
                    case "product_metrics" -> {
                        stmt.execute(props.getProperty("sql.create.table"));
                        stmt.execute(props.getProperty("sql.create.index"));
                        stmt.execute(props.getProperty("sql.create.hypertable"));
                    }
                    case "key_market_list" -> {
                        stmt.execute(props.getProperty("sql.create.table1"));
                        stmt.execute(props.getProperty("sql.create.index1"));
                        stmt.execute(props.getProperty("sql.create.hypertable1"));
                    }
                    case "key_customer_list" -> {
                        stmt.execute(props.getProperty("sql.create.table2"));
                        stmt.execute(props.getProperty("sql.create.index2"));
                        stmt.execute(props.getProperty("sql.create.hypertable2"));
                    }
                }

                for (String bucketType : timeBuckets) {
                    String bucketValue = timeBucketMap.getOrDefault(bucketType.toLowerCase(), "");
                    List<String> rawParts = new ArrayList<>();
                    StringBuilder valueBuilder = new StringBuilder();

                    for (String attr : bucketAttributes) {
                        JsonNode valNode = input.get(attr);
                        String val = (valNode != null) ? valNode.asText() : "";
                        rawParts.add(val);
                        valueBuilder.append(val);
                    }

                    valueBuilder.append(bucketValue).append(year);
                    rawParts.add(bucketValue);
                    rawParts.add(String.valueOf(year));

                    String key = String.join("_", bucketAttributes) + "_" + bucketType + "_year";
                    String metadataJson = mapper.writeValueAsString(Map.of(key, valueBuilder.toString()));
                    String attributesJson = mapper.writeValueAsString(attributeList);
                    String hashInput = String.join(":", rawParts);

                    saveToDatabase(entity, timestamp, metadataJson, hashInput, attributesJson, props);
                }
            }

            logger.info("Successfully inserted data for entity: {}", entity);
            return "Inserted successfully";

        } catch (Exception e) {
            logger.error("Error in runJob for entity {}: {}", entity, e.getMessage(), e);
            return "Failed: " + e.getMessage();
        } finally {
            logger.info("Exiting runJob for entity: {}", entity);
        }
    }

    private void saveToDatabase(String entity, Timestamp time, String metadataJson, String hashKey, String attributesJson, Properties props) {
        logger.info("Entering saveToDatabase for entity: {}", entity);
        String insertQuery;

        if (entity.equals("product_metrics")) insertQuery = props.getProperty("sql.insert");
        else if (entity.equals("key_market_list")) insertQuery = props.getProperty("sql.insert1");
        else insertQuery = props.getProperty("sql.insert2");

        try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
             PreparedStatement ps = conn.prepareStatement(insertQuery)) {

            ps.setTimestamp(1, time);
            ps.setString(2, metadataJson);
            ps.setString(3, hashKey);
            ps.setString(4, attributesJson);
            ps.executeUpdate();
            logger.info("Data inserted into {} successfully", entity);

        } catch (Exception e) {
            logger.error("Error inserting data into {}: {}", entity, e.getMessage(), e);
        } finally {
            logger.info("Exiting saveToDatabase for entity: {}", entity);
        }
    }

    private Map<String, String> buildTimeBucketMap(int month, int year, int quarterly, int half, int financialYear) {
        logger.info("Entering buildTimeBucketMap");
        Map<String, String> map = new HashMap<>();
        map.put("monthly", String.valueOf(month));
        map.put("quarterly", "Q" + quarterly);
        map.put("halfyearly", "H" + half);
        map.put("annually", String.valueOf(year));
        map.put("financialyear", "FY" + financialYear);
        logger.info("Exiting buildTimeBucketMap");
        return map;
    }

    private String parseDate(JsonNode input) {
        logger.info("Entering parseDate");
        try {
            if (input.has("date")) return input.get("date").asText();
            if (input.has("year") && input.has("time_bucket")) {
                String year = input.get("year").asText();
                String tb = input.get("time_bucket").asText().toUpperCase();
                return year + "-" + switch (tb) {
                    case "Q1" -> "01";
                    case "Q2" -> "04";
                    case "Q3" -> "07";
                    case "Q4" -> "10";
                    default -> "01";
                } + "-01";
            }
            return input.get("year").asText() + "-01-01";
        } catch (Exception e) {
            logger.error("Error in parseDate: {}", e.getMessage(), e);
            return "1970-01-01"; // fallback
        } finally {
            logger.info("Exiting parseDate");
        }
    }

    private Map<String, Object> buildAttributes(JsonNode attributesNode) {
        logger.info("Entering buildAttributes");
        Map<String, Object> result = new HashMap<>();
        try {
            for (JsonNode attr : attributesNode) {
                String name = attr.get("attributeName").asText();
                String type = attr.get("type").asText();
                switch (type) {
                    case "string" -> result.put(name, "");
                    case "array" -> {
                        List<Map<String, Object>> list = new ArrayList<>();
                        JsonNode obj = attr.get("attributes").get(0);
                        Map<String, Object> sub = new HashMap<>();
                        obj.fieldNames().forEachRemaining(f -> sub.put(f, ""));
                        list.add(sub);
                        result.put(name, list);
                    }
                    case "object" -> {
                        Map<String, Object> map = new HashMap<>();
                        JsonNode obj = attr.get("attributes");
                        obj.fieldNames().forEachRemaining(f -> map.put(f, ""));
                        result.put(name, map);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in buildAttributes: {}", e.getMessage(), e);
        } finally {
            logger.info("Exiting buildAttributes");
        }
        return result;
    }

    private String validateInput(JsonNode input) {
        logger.info("Entering validateInput");
        try {
            List<String> requiredArrays = List.of("timeBucketKey", "timeBucket");
            List<String> requiredFields = List.of("entity", "entityType", "attributes");

            for (String field : requiredFields) {
                if (!input.has(field)) return "Missing required field: " + field;
            }

            for (String arrayField : requiredArrays) {
                if (!input.has(arrayField) || !input.get(arrayField).isArray() || input.get(arrayField).isEmpty()) {
                    return "Missing or empty array field: " + arrayField;
                }
            }

            for (JsonNode attr : input.get("attributes")) {
                if (!attr.has("attributeName") || !attr.has("type")) {
                    return "Each attribute must have 'attributeName' and 'type'";
                }
            }

            return null;
        } catch (Exception e) {
            logger.error("Error in validateInput: {}", e.getMessage(), e);
            return "Invalid input structure";
        } finally {
            logger.info("Exiting validateInput");
        }
    }
}
