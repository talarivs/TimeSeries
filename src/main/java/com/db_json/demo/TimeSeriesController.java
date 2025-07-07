package com.db_json.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.io.File;
import java.nio.file.Files;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class TimeSeriesController {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesController.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public String processJsonFile(String filePath, String entityName) {
        try {
            logger.info("Reading JSON input from file: {}", filePath);
            String inputJson = new String(Files.readAllBytes(new File(filePath).toPath()));
            return processRequest(inputJson, entityName);
        } catch (Exception e) {
            logger.error("Failed to read or process JSON file: {}", e.getMessage(), e);
            return "Error: " + e.getMessage();
        }
    }

    private String processRequest(String inputJson, String entity) {
        logger.info("Processing request for entity: {}", entity);
        try {
            JsonNode input = objectMapper.readTree(inputJson);
            logger.debug("Parsed input JSON: {}", input);
            TimeSeriesInputValidator.validate(input);
            logger.info("Input JSON validated successfully.");

            List<String> bucketAttributes = Optional.ofNullable(input.get("timeBucketKey"))
                    .map(this::jsonArrayToList)
                    .orElse(Collections.emptyList());
            List<String> timeBuckets = Optional.ofNullable(input.get("timeBucket"))
                    .map(this::jsonArrayToList)
                    .orElse(Collections.emptyList());

            logger.info("Time bucket attributes: {}", bucketAttributes);
            logger.info("Time buckets requested: {}", timeBuckets);

            Map<String, Object> attributeList = extractAttributes(input.get("attributes"));
            logger.debug("Parsed attribute list: {}", attributeList);

            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(getDate(input));
            Timestamp timestamp = new Timestamp(date.getTime());
            logger.info("Parsed timestamp: {}", timestamp);

            Map<String, String> timeBucketMap = getTimeBuckets(date);
            logger.debug("Resolved time bucket map: {}", timeBucketMap);

            Properties props = loadProperties();
            logger.info("Properties loaded successfully.");

            Map<String, EntityConfig> entityConfigs = loadEntityConfigs(props);
            EntityConfig config = entityConfigs.get(entity);
            if (config == null) {
                logger.error("No configuration found for entity: {}", entity);
                return "Configuration missing for entity: " + entity;
            }

            setupDatabase(config, props);
            logger.info("Database setup complete for entity: {}", entity);

            timeBuckets.forEach(bucket -> handleInsertion(bucket, input, config, props,
                    bucketAttributes, timeBucketMap, attributeList, timestamp));

            logger.info("Data insertion completed successfully for entity: {}", entity);
            return "Inserted successfully";

        } catch (Exception ex) {
            logger.error("Unexpected error: {}", ex.getMessage(), ex);
            return "Failed: " + ex.getMessage();
        }
    }

    private List<String> jsonArrayToList(JsonNode arrayNode) {
        return IntStream.range(0, arrayNode.size())
                .mapToObj(i -> arrayNode.get(i).asText())
                .collect(Collectors.toList());
    }

    private String getDate(JsonNode input) {
        return Optional.ofNullable(input.get("date"))
                .map(JsonNode::asText)
                .orElseGet(() -> getDefaultDate(input));
    }

    private String getDefaultDate(JsonNode input) {
        String year = input.has("year") ? input.get("year").asText() : "1970";
        String tb = input.has("time_bucket") ? input.get("time_bucket").asText().toUpperCase() : "Q1";
        return switch (tb) {
            case "Q2" -> year + "-04-01";
            case "Q3" -> year + "-07-01";
            case "Q4" -> year + "-10-01";
            default -> year + "-01-01";
        };
    }

    private Map<String, String> getTimeBuckets(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH) + 1;
        int year = cal.get(Calendar.YEAR);
        return Map.of(
                "monthly", String.valueOf(month),
                "quarterly", "Q" + ((month - 1) / 3 + 1),
                "halfyearly", "H" + ((month - 1) / 6 + 1),
                "annually", String.valueOf(year),
                "financialyear", "FY" + (month >= 4 ? year + 1 : year)
        );
    }

    private void setupDatabase(EntityConfig config, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                props.getProperty("db.url"),
                props.getProperty("db.user"),
                props.getProperty("db.password"));
             Statement stmt = conn.createStatement()) {

            stmt.execute(props.getProperty(config.createTableKey()));
            stmt.execute(props.getProperty(config.createIndexKey()));
            stmt.execute(props.getProperty(config.createHypertableKey()));
        }
    }

    private void handleInsertion(String bucketType, JsonNode input, EntityConfig config, Properties props,
                                 List<String> bucketAttributes, Map<String, String> timeBucketMap,
                                 Map<String, Object> attributeList, Timestamp timestamp) {

        String bucketValue = timeBucketMap.getOrDefault(bucketType.toLowerCase(), "");
        List<String> rawParts = bucketAttributes.stream()
                .map(attr -> Optional.ofNullable(input.get(attr)).map(JsonNode::asText).orElse(""))
                .collect(Collectors.toList());

        StringBuilder keyBuilder = new StringBuilder();
        rawParts.forEach(keyBuilder::append);
        keyBuilder.append(bucketValue);

        String compoundKey = keyBuilder.toString();
        String key = String.join("_", bucketAttributes) + "_" + bucketType + "_year";
        String metadataJson = toJson(Map.of(key, compoundKey + Calendar.getInstance().get(Calendar.YEAR)));
        String attributesJson = toJson(attributeList);
        String hashKey = String.join(":", rawParts) + ":" + bucketValue + ":" + Calendar.getInstance().get(Calendar.YEAR);

        logger.info("Compound key: {}", compoundKey);
        logger.info("Final hash key: {}", hashKey);
        logger.debug("Metadata JSON: {}", metadataJson);
        logger.debug("Attribute JSON: {}", attributesJson);

        saveRecord(timestamp, metadataJson, hashKey, attributesJson, props, config);
    }

    private void saveRecord(Timestamp time, String metadataJson, String hashKey,
                            String attributesJson, Properties props, EntityConfig config) {
        try (Connection conn = DriverManager.getConnection(
                props.getProperty("db.url"),
                props.getProperty("db.user"),
                props.getProperty("db.password"));
             PreparedStatement ps = conn.prepareStatement(props.getProperty(config.insertKey()))) {

            ps.setTimestamp(1, time);
            ps.setString(2, metadataJson);
            ps.setString(3, hashKey);
            ps.setString(4, attributesJson);
            ps.executeUpdate();
            logger.info("Record inserted successfully.");
        } catch (Exception ex) {
            logger.error("Error inserting data: {}", ex.getMessage(), ex);
        }
    }

    private Properties loadProperties() throws Exception {
        Properties props = new Properties();
        props.load(TimeSeriesController.class.getClassLoader().getResourceAsStream("application.properties"));
        return props;
    }

    private Map<String, EntityConfig> loadEntityConfigs(Properties props) {
        return Arrays.stream(props.getProperty("entities", "").split(","))
                .filter(entity -> !entity.isBlank())
                .collect(Collectors.toMap(
                        entity -> entity,
                        entity -> new EntityConfig(
                                props.getProperty(entity + ".createTable"),
                                props.getProperty(entity + ".createIndex"),
                                props.getProperty(entity + ".createHypertable"),
                                props.getProperty(entity + ".insert")
                        )
                ));
    }

    private Map<String, Object> extractAttributes(JsonNode attributesNode) {
        if (attributesNode == null || !attributesNode.isArray()) return Collections.emptyMap();

        return IntStream.range(0, attributesNode.size())
                .mapToObj(attributesNode::get)
                .collect(Collectors.toMap(
                        attr -> attr.get("attributeName").asText(),
                        this::parseAttribute,
                        (v1, v2) -> v1
                ));
    }

    private Object parseAttribute(JsonNode attrNode) {
        String type = attrNode.get("type").asText().toLowerCase();
        JsonNode values = attrNode.get("attributes");

        if ("array".equals(type) && values.isArray()) {
            return IntStream.range(0, values.size())
                    .mapToObj(values::get)
                    .map(node -> objectMapper.convertValue(node, Map.class))
                    .collect(Collectors.toList());
        } else if ("object".equals(type) && values.isObject()) {
            return objectMapper.convertValue(values, Map.class);
        } else if ("string".equals(type)) {
            return values.isObject() ? objectMapper.convertValue(values, Map.class) : values.asText();
        }
        return "";
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            logger.error("Error serializing object: {}", e.getMessage(), e);
            return "{}";
        }
    }

    private static class EntityConfig {
        private final String createTableKey;
        private final String createIndexKey;
        private final String createHypertableKey;
        private final String insertKey;

        public EntityConfig(String createTableKey, String createIndexKey, String createHypertableKey, String insertKey) {
            this.createTableKey = createTableKey;
            this.createIndexKey = createIndexKey;
            this.createHypertableKey = createHypertableKey;
            this.insertKey = insertKey;
        }

        public String createTableKey() { return createTableKey; }
        public String createIndexKey() { return createIndexKey; }
        public String createHypertableKey() { return createHypertableKey; }
        public String insertKey() { return insertKey; }
    }
}
