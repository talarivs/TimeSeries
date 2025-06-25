package com.db_json.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

public class JSONtoDB1 {

    public static Properties loadConfig() throws Exception {
        Properties props = new Properties();
        try (InputStream input = JSONtoDB1.class.getClassLoader().getResourceAsStream("JSONtoDB1_config.properties")) {
            if (input == null) throw new RuntimeException("Config file not found.");
            props.load(input);
        }
        return props;
    }

    public static JsonNode loadSchemaJson(String jsonFileKey, Properties props) throws Exception {
        String fileName = props.getProperty(jsonFileKey);
        InputStream schemaStream = JSONtoDB1.class.getClassLoader().getResourceAsStream(fileName);
        if (schemaStream == null) throw new RuntimeException("Schema JSON file not found: " + fileName);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(schemaStream);
    }

    public static Map<String, Object> buildAttributesFromSchema(JsonNode schemaNode) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        JsonNode attributesNode = schemaNode.get("attributes");
        if (attributesNode == null || !attributesNode.isArray()) {
            throw new RuntimeException("Missing or invalid 'attributes' field in attribute schema JSON.");
        }
        for (JsonNode attr : attributesNode) {
            String name = attr.get("attributeName").asText();
            String type = attr.get("type").asText();
            JsonNode nested = attr.get("attributes");
            switch (type) {
                case "array":
                    Map<String, Object> nestedMap = new HashMap<>();
                    if (nested != null && nested.isArray()) {
                        for (JsonNode field : nested) {
                            field.fieldNames().forEachRemaining(key -> nestedMap.put(key, ""));
                        }
                    }
                    jsonMap.put(name, List.of(nestedMap));
                    break;
                case "object":
                    Map<String, Object> objectMap = new HashMap<>();
                    if (nested != null && nested.isObject()) {
                        nested.fieldNames().forEachRemaining(key -> objectMap.put(key, ""));
                    }
                    jsonMap.put(name, objectMap);
                    break;
                case "string":
                    jsonMap.put(name, "");
                    break;
            }
        }
        return jsonMap;
    }

    public static void createTableAndIndex(Properties props) {
        try (Connection conn = DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));
             Statement stmt = conn.createStatement()) {
            stmt.execute(props.getProperty("sql.create.table"));
            stmt.execute(props.getProperty("sql.create.index"));
            System.out.println("Table and index created or already exist.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> getTimeBuckets(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int month = calendar.get(Calendar.MONTH) + 1;
        int quarter = (month - 1) / 3 + 1;
        int half = (month - 1) / 6 + 1;
        int year = calendar.get(Calendar.YEAR);
        List<String> buckets = new ArrayList<>();
        buckets.add("month:" + month);
        buckets.add("quarter:Q" + quarter);
        buckets.add("half:H" + half);
        buckets.add("year:" + year);
        buckets.add("financialYear:FY" + (month >= 4 ? year + 1 : year));
        return buckets;
    }

    public static void runFlinkJob(String productId, String region, String dateStr, String year) throws Exception {
        Properties props = loadConfig();
        createTableAndIndex(props);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode attributeSchema = loadSchemaJson("json.file1", props);
        Map<String, Object> attributesJson = buildAttributesFromSchema(attributeSchema);

        Date inputDate = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
        String actualYear = new SimpleDateFormat("yyyy").format(inputDate);
        List<String> timeBuckets = getTimeBuckets(inputDate);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> rowStream = env.fromElements("input").flatMap(new RichFlatMapFunction<String, Row>() {
            @Override
            public void flatMap(String value, Collector<Row> out) throws Exception {
                for (String bucket : timeBuckets) {
                    String[] parts = bucket.split(":");
                    String bucketType = parts[0];
                    String bucketValue = parts[1];

                    String metadataKey;
                    String metadataValue;

                    switch (bucketType) {
                        case "month":
                            metadataKey = "productId_region_month_year";
                            metadataValue = productId + region + bucketValue + actualYear;
                            break;
                        case "quarter":
                            metadataKey = "productId_region_quarter_year";
                            metadataValue = productId + bucketValue + region + actualYear;
                            break;
                        case "half":
                            metadataKey = "productId_region_half_year";
                            metadataValue = productId + bucketValue + region + actualYear;
                            break;
                        case "year":
                            metadataKey = "productId_region_year_year";
                            metadataValue = productId + region + actualYear;
                            break;
                        case "financialYear":
                            metadataKey = "productId_region_financialyear_year";
                            metadataValue = productId + bucketValue + region + actualYear;
                            break;
                        default:
                            continue;
                    }

                    String jsonMetadata = mapper.writeValueAsString(Collections.singletonMap(metadataKey, metadataValue));
                    String hashKey = sha256Hex(productId + ":" + region + ":" + bucketValue + ":" + actualYear).substring(0, 32);

                    Row row = new Row(3);
                    row.setField(0, mapper.readTree(jsonMetadata)); // hashkey_metadata (JSONB)
                    row.setField(1, hashKey); // time_bucket_key
                    row.setField(2, mapper.readTree(mapper.writeValueAsString(attributesJson))); // attribute_list (JSONB)
                    out.collect(row);
                }
            }
        });

        rowStream.addSink(JdbcSink.sink(
                props.getProperty("sql.insert"),
                (ps, row) -> {
                    ps.setObject(1, row.getField(0), java.sql.Types.OTHER); // hashkey_metadata (JSONB)
                    ps.setString(2, (String) row.getField(1));               // time_bucket_key
                    ps.setObject(3, row.getField(2), java.sql.Types.OTHER); // attribute_list (JSONB)
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(props.getProperty("db.url"))
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(props.getProperty("db.user"))
                        .withPassword(props.getProperty("db.password"))
                        .build()
        ));

        env.execute("Flink Job: Insert Product Metrics");
    }
}
