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
        logger.info("Triggered endpoint: /api/timeseries/run");
        String result = runJob(inputJson, "product_metrics");
        logger.info("Completed endpoint: /api/timeseries/run");
        return result;
    }

    @PostMapping("/customerdata/run")
    public String runCustomerDataJob(@RequestBody String inputJson) {
        logger.info("Triggered endpoint: /api/customerdata/run");
        String result = runJob(inputJson, "key_customer_list");
        logger.info("Completed endpoint: /api/customerdata/run");
        return result;
    }

    @PostMapping("/marketdata/run")
    public String runMarketDataJob(@RequestBody String inputJson) {
        logger.info("Triggered endpoint: /api/marketdata/run");
        String result = runJob(inputJson, "key_market_list");
        logger.info("Completed endpoint: /api/marketdata/run");
        return result;
    }

    private String runJob(String inputJson, String entity) {
        logger.info("Starting job execution for entity: {}", entity);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode input = mapper.readTree(inputJson);

            logger.info("Received input fields:");
            input.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                if (value.isValueNode()) {
                    logger.info(" - {}: {}", key, value.asText());
                } else if (value.isArray()) {
                    logger.info(" - {} (Array):", key);
                    for (int i = 0; i < value.size(); i++) {
                        logger.info("     [{}]: {}", i, value.get(i).toPrettyString());
                    }
                } else if (value.isObject()) {
                    logger.info(" - {} (Object): {}", key, value.toPrettyString());
                } else {
                    logger.info(" - {} (Unknown type): {}", key, value.toString());
                }
            });

            try {
                TimeSeriesInputValidator.validate(input);
                logger.info("Input validation passed.");
            } catch (InvalidInputException e) {
                logger.error("Input validation failed: {}", e.getMessage());
                return "Invalid input: " + e.getMessage();
            }

            List<String> bucketAttributes = new ArrayList<>();
            input.get("timeBucketKey").forEach(n -> bucketAttributes.add(n.asText()));
            List<String> timeBuckets = new ArrayList<>();
            input.get("timeBucket").forEach(n -> timeBuckets.add(n.asText()));

            logger.info("Extracted timeBucketKey: {}", bucketAttributes);
            logger.info("Extracted timeBucket: {}", timeBuckets);

            Map<String, Object> attributeList = extractAndValidateAttributes(input.get("attributes"));

            String dateStr = parseDate(input);
            logger.info("Parsed date: {}", dateStr);
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
            Timestamp timestamp = new Timestamp(date.getTime());
            logger.info("Computed timestamp: {}", timestamp);

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

            try (Connection conn = DriverManager.getConnection(
                    props.getProperty("db.url"),
                    props.getProperty("db.user"),
                    props.getProperty("db.password"));
                 Statement stmt = conn.createStatement()) {

                logger.info("Connected to database. Preparing to create table, index, and hypertable for entity: {}", entity);

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
                    logger.info("Processing time bucket: {}, resolved value: {}", bucketType, bucketValue);

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

                    logger.info("Generated hashInput: {}", hashInput);
                    logger.debug("Constructed metadata JSON: {}", metadataJson);
                    logger.debug("Constructed attributes JSON: {}", attributesJson);

                    saveToDatabase(entity, timestamp, metadataJson, hashInput, attributesJson, props);
                }
            }

            logger.info("Data successfully inserted for entity: {}", entity);
            return "Inserted successfully";

        } catch (Exception e) {
            logger.error("Error executing job for entity {}: {}", entity, e.getMessage(), e);
            return "Failed: " + e.getMessage();
        } finally {
            logger.info("Completed runJob for entity: {}", entity);
        }
    }

    private void saveToDatabase(String entity, Timestamp time, String metadataJson, String hashKey, String attributesJson, Properties props) {
        logger.info("Inserting data into entity: {}", entity);
        String insertQuery;

        if (entity.equals("product_metrics")) insertQuery = props.getProperty("sql.insert");
        else if (entity.equals("key_market_list")) insertQuery = props.getProperty("sql.insert1");
        else insertQuery = props.getProperty("sql.insert2");

        logger.debug("Insert query: {}", insertQuery);

        try (Connection conn = DriverManager.getConnection(
                props.getProperty("db.url"),
                props.getProperty("db.user"),
                props.getProperty("db.password"));
             PreparedStatement ps = conn.prepareStatement(insertQuery)) {

            ps.setTimestamp(1, time);
            ps.setString(2, metadataJson);
            ps.setString(3, hashKey);
            ps.setString(4, attributesJson);
            ps.executeUpdate();

            logger.info("Insert successful for entity: {}", entity);

        } catch (Exception e) {
            logger.error("Error inserting into entity {}: {}", entity, e.getMessage(), e);
        }
    }

    private Map<String, String> buildTimeBucketMap(int month, int year, int quarterly, int half, int financialYear) {
        logger.info("Building time bucket map");
        Map<String, String> map = new HashMap<>();
        map.put("monthly", String.valueOf(month));
        map.put("quarterly", "Q" + quarterly);
        map.put("halfyearly", "H" + half);
        map.put("annually", String.valueOf(year));
        map.put("financialyear", "FY" + financialYear);
        return map;
    }

    private String parseDate(JsonNode input) {
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
            logger.error("Error parsing date: {}", e.getMessage(), e);
            return "1970-01-01";
        }
    }

    private Map<String, Object> extractAndValidateAttributes(JsonNode attributesNode) throws InvalidInputException {
        logger.info("Extracting and validating dynamic attributes");
        Map<String, Object> result = new HashMap<>();

        for (JsonNode attr : attributesNode) {
            String attrName = attr.get("attributeName").asText();
            String attrType = attr.get("type").asText();
            JsonNode attributesValue = attr.get("attributes");

            switch (attrType.toLowerCase()) {
                case "array" -> {
                    if (attributesValue != null && attributesValue.isArray()) {
                        List<Map<String, Object>> list = new ArrayList<>();
                        for (JsonNode item : attributesValue) {
                            Map<String, Object> itemMap = new HashMap<>();
                            for (Iterator<String> fields = item.fieldNames(); fields.hasNext(); ) {
                                String field = fields.next();
                                JsonNode fieldSchema = attr.get(field);
                                JsonNode fieldValue = item.get(field);
                                if (fieldSchema != null && fieldSchema.has("type")) {
                                    String expectedType = fieldSchema.get("type").asText();
                                    if (expectedType.equals("number") && !fieldValue.isNumber()) {
                                        throw new InvalidInputException("Field '" + field + "' in '" + attrName + "' must be an integer.");
                                    }
                                }
                                itemMap.put(field, fieldValue.asText());
                            }
                            list.add(itemMap);
                        }
                        result.put(attrName, list);
                    }
                }

                case "object", "string" -> {
                    if (attributesValue != null && attributesValue.isObject()) {
                        Map<String, Object> objMap = new HashMap<>();
                        for (Iterator<String> fields = attributesValue.fieldNames(); fields.hasNext(); ) {
                            String field = fields.next();
                            JsonNode fieldSchema = attr.get(field);
                            JsonNode fieldValue = attributesValue.get(field);
                            if (fieldSchema != null && fieldSchema.has("type")) {
                                String expectedType = fieldSchema.get("type").asText();
                                if (expectedType.equals("number") && !fieldValue.isNumber()) {
                                    throw new InvalidInputException("Field '" + field + "' in '" + attrName + "' must be an integer.");
                                }
                            }
                            objMap.put(field, fieldValue.asText());
                        }
                        result.put(attrName, objMap);
                    }
                }

                default -> logger.warn("Unsupported attribute type: {}", attrType);
            }
        }

        logger.info("Attribute extraction completed");
        return result;
    }
}
