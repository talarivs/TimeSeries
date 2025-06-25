package com.example.demo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class JSONtoDB {

    public static Properties loadConfig() throws Exception {
        Properties props = new Properties();
        try (InputStream input = JSONtoDB.class.getClassLoader().getResourceAsStream("JSONtoDB_config.properties")) {
            if (input == null) {
                throw new RuntimeException("application.properties file not found");
            }
            props.load(input);
        }
        return props;
    }

    public static JsonNode loadSchemaJson(Properties props) throws Exception {
        String jsonSchemaPath = props.getProperty("json.schema.file");
        InputStream schemaStream = JSONtoDB.class.getClassLoader().getResourceAsStream(jsonSchemaPath);
        if (schemaStream == null) {
            throw new RuntimeException("Schema JSON file not found: " + jsonSchemaPath);
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(schemaStream);
    }

    public static Map<String, Object> buildEmptyJson(JsonNode schemaNode) {
        Map<String, Object> jsonMap = new HashMap<>();

        for (JsonNode attr : schemaNode.get("attributes")) {
            String name = attr.get("attributeName").asText();
            String type = attr.get("type").asText();
            JsonNode nested = attr.get("attributes");

            switch (type) {
                case "array":
                    if (nested != null && nested.isArray()) {
                        Map<String, Object> nestedMap = new HashMap<>();
                        for (JsonNode field : nested) {
                            Iterator<String> keys = field.fieldNames();
                            while (keys.hasNext()) {
                                String key = keys.next();
                                nestedMap.put(key, "");
                            }
                        }
                        jsonMap.put(name, Collections.singletonList(nestedMap));
                    } else {
                        jsonMap.put(name, new ArrayList<>());
                    }
                    break;

                case "object":
                    Map<String, Object> objectMap = new HashMap<>();
                    if (nested != null && nested.isObject()) {
                        Iterator<String> keys = nested.fieldNames();
                        while (keys.hasNext()) {
                            String key = keys.next();
                            objectMap.put(key, "");
                        }
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

    public static boolean validateJson(Map<String, Object> json, JsonNode schema) {
        for (JsonNode attr : schema.get("attributes")) {
            String name = attr.get("attributeName").asText();
            String type = attr.get("type").asText();

            if (!json.containsKey(name)) {
                System.err.println("Missing attribute: " + name);
                return false;
            }

            Object value = json.get(name);
            switch (type) {
                case "string":
                    if (!(value instanceof String)) {
                        System.err.println("Invalid type for " + name + ": expected string");
                        return false;
                    }
                    break;
                case "object":
                    if (!(value instanceof Map)) {
                        System.err.println("Invalid type for " + name + ": expected object");
                        return false;
                    }
                    break;
                case "array":
                    if (!(value instanceof List)) {
                        System.err.println("Invalid type for " + name + ": expected array");
                        return false;
                    }
                    break;
                default:
                    System.err.println("Unknown type: " + type);
                    return false;
            }
        }
        return true;
    }

    public static void createTable(Properties props) {
        String url = props.getProperty("db.url");
        String user = props.getProperty("db.user");
        String password = props.getProperty("db.password");
        String createTableSQL = props.getProperty("sql.create.table");

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("Table 'product_metrics' ensured.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = loadConfig();
        JsonNode schemaJson = loadSchemaJson(props);
        createTable(props);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, Object> dummyJson = buildEmptyJson(schemaJson);
        if (!validateJson(dummyJson, schemaJson)) {
            throw new RuntimeException("Validation failed for generated JSON.");
        }

        DataStream<Map<String, Object>> input = env.fromElements(dummyJson);

        DataStream<Row> rowStream = input.map(new RichMapFunction<Map<String, Object>, Row>() {
            private transient ObjectMapper mapper;

            @Override
            public void open(Configuration parameters) {
                mapper = new ObjectMapper();
            }

            @Override
            public Row map(Map<String, Object> value) throws Exception {
                Row row = new Row(2);
                String entityType = schemaJson.get("entityType").asText();
                String entityTypeHash = DigestUtils.md5Hex(entityType);
                row.setField(0, "\"" + entityTypeHash + "\"");
                row.setField(1, mapper.writeValueAsString(value));
                return row;
            }
        });

        rowStream.addSink(JdbcSink.sink(
                props.getProperty("sql.insert"),
                (PreparedStatement ps, Row row) -> {
                    ps.setString(1, row.getField(0).toString());
                    ps.setObject(2, row.getField(1)); // Postgres will auto-cast JSONB
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(props.getProperty("db.url"))
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(props.getProperty("db.user"))
                        .withPassword(props.getProperty("db.password"))
                        .build()
        ));

        JdbcInputFormat inputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl(props.getProperty("db.url"))
                .setUsername(props.getProperty("db.user"))
                .setPassword(props.getProperty("db.password"))
                .setQuery(props.getProperty("sql.select"))
                .setRowTypeInfo(new RowTypeInfo(Types.STRING))
                .finish();

        env.createInput(inputFormat)
                .map((Row row) -> {
                    ObjectMapper mapper = new ObjectMapper();
                    String jsonStr = row.getField(0).toString();
                    return mapper.readValue(jsonStr, new TypeReference<Map<String, Object>>() {});
                })
                .returns(Types.MAP(Types.STRING, Types.GENERIC(Object.class)))
                .print();

        env.execute("JSON to PostgreSQL");
    }
}
