//package com.db_json.demo;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//
//public class TimeSeriesInputValidator {
//
//    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesInputValidator.class);
//
//    public static void validate(JsonNode input) {
//        logger.info("Validating input JSON...");
//
//        if (input == null || input.isEmpty()) {
//            throw new InvalidInputException("Input JSON is empty or null.");
//        }
//
//        // Required top-level fields
//        for (String key : List.of("entity", "entityType", "attributes")) {
//            if (!input.has(key)) {
//                throw new InvalidInputException("Missing required field: " + key);
//            }
//        }
//
//        // Required arrays
//        for (String key : List.of("timeBucketKey", "timeBucket")) {
//            if (!input.has(key) || !input.get(key).isArray() || input.get(key).isEmpty()) {
//                throw new InvalidInputException("Missing or empty array field: " + key);
//            }
//        }
//
//        // Validate attributes dynamically
//        for (JsonNode attr : input.get("attributes")) {
//            if (!attr.has("attributeName") || !attr.has("type")) {
//                throw new InvalidInputException("Each attribute must have 'attributeName' and 'type'");
//            }
//
//            String attrName = attr.get("attributeName").asText();
//            String attrType = attr.get("type").asText();
//            JsonNode attrValues = attr.get("attributes");
//
//            if (attrValues == null) continue;
//
//            if (attrType.equalsIgnoreCase("array") && attrValues.isArray()) {
//                for (JsonNode item : attrValues) {
//                    validateWithMetadata(attrName, item, attr);
//                }
//            } else if (attrType.equalsIgnoreCase("object") && attrValues.isObject()) {
//                validateWithMetadata(attrName, attrValues, attr);
//            } else {
//                // Treat as primitive value
//                validateWithMetadata(attrName, attrValues, attr);
//            }
//        }
//
//        logger.info("Input validation passed successfully.");
//    }
//
//    private static void validateWithMetadata(String attrName, JsonNode valueNode, JsonNode metadataNode) {
//        for (Iterator<String> it = metadataNode.fieldNames(); it.hasNext(); ) {
//            String fieldName = it.next();
//
//            if (Set.of("attributeName", "type", "attributes").contains(fieldName)) continue;
//
//            JsonNode fieldSchema = metadataNode.get(fieldName);
//            JsonNode actualValue = valueNode.get(fieldName);
//
//            if (!fieldSchema.isObject()) continue;
//
//            // Validate mandatory
//            boolean isMandatory = fieldSchema.path("mandatory").asBoolean(false);
//            if (isMandatory && (actualValue == null || actualValue.isNull())) {
//                throw new InvalidInputException("Missing mandatory field '" + fieldName + "' in attribute '" + attrName + "'");
//            }
//
//            // Validate type
//            String expectedType = fieldSchema.path("type").asText();
//            if (actualValue != null && !actualValue.isNull()) {
//                if (expectedType.equalsIgnoreCase("number")) {
//                    try {
//                        Double.parseDouble(actualValue.asText()); // supports both number and numeric strings
//                    } catch (NumberFormatException e) {
//                        throw new InvalidInputException("Field '" + fieldName + "' in '" + attrName + "' is not a valid number: " + actualValue);
//                    }
//                } else if (expectedType.equalsIgnoreCase("string")) {
//                    if (!actualValue.isTextual()) {
//                        throw new InvalidInputException("Field '" + fieldName + "' in '" + attrName + "' is not a string: " + actualValue);
//                    }
//                }
//                // Extend with more types as needed
//            }
//
//            // Validate validValues
//            JsonNode validValues = fieldSchema.path("validValues");
//            if (validValues.isArray() && validValues.size() > 0 && actualValue != null && !actualValue.isNull()) {
//                boolean isValid = false;
//                for (JsonNode valid : validValues) {
//                    if (valid.asText().equals(actualValue.asText())) {
//                        isValid = true;
//                        break;
//                    }
//                }
//                if (!isValid) {
//                    throw new InvalidInputException("Invalid value '" + actualValue.asText() + "' for field '" + fieldName + "' in '" + attrName + "'");
//                }
//            }
//        }
//    }
//}


package com.db_json.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimeSeriesInputValidator {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesInputValidator.class);

    public static void validate(JsonNode input) {
        logger.info("Validating input JSON...");

        if (input == null || input.isEmpty()) {
            throw new InvalidInputException("Input JSON is empty or null.");
        }

        // Required top-level fields
        for (String key : List.of("entity", "entityType", "attributes")) {
            if (!input.has(key)) {
                throw new InvalidInputException("Missing required field: " + key);
            }
        }

        // Required arrays
        for (String key : List.of("timeBucketKey", "timeBucket")) {
            if (!input.has(key) || !input.get(key).isArray() || input.get(key).isEmpty()) {
                throw new InvalidInputException("Missing or empty array field: " + key);
            }
        }

        // Validate attributes dynamically
        for (JsonNode attr : input.get("attributes")) {
            if (!attr.has("attributeName") || !attr.has("type")) {
                throw new InvalidInputException("Each attribute must have 'attributeName' and 'type'");
            }

            String attrName = attr.get("attributeName").asText();
            String attrType = attr.get("type").asText();
            JsonNode attrValues = attr.get("attributes");

            switch (attrType.toLowerCase()) {
                case "array" -> {
                    if (attrValues != null && attrValues.isArray()) {
                        for (JsonNode item : attrValues) {
                            validateWithMetadata(attrName, item, attr);
                        }
                    }
                }
                case "object" -> {
                    if (attrValues != null && attrValues.isObject()) {
                        validateWithMetadata(attrName, attrValues, attr);
                    }
                }
                case "string" -> {
                    if (attrValues != null) {
                        if (attrValues.isTextual()) {
                            // Example: "attributes": "name, weight, score_value"
                            String[] fields = attrValues.asText().split(",");
                            for (String field : fields) {
                                String fieldName = field.trim();
                                JsonNode syntheticNode = buildFieldJsonNode(fieldName, attr);
                                validateWithMetadata(attrName, syntheticNode, attr);
                            }
                        } else {
                            // Treat it like a map
                            validateWithMetadata(attrName, attrValues, attr);
                        }
                    }
                }
                default -> throw new InvalidInputException("Unsupported attribute type: " + attrType);
            }
        }

        logger.info("Input validation passed successfully.");
    }

    private static void validateWithMetadata(String attrName, JsonNode valueNode, JsonNode metadataNode) {
        for (Iterator<String> it = metadataNode.fieldNames(); it.hasNext(); ) {
            String fieldName = it.next();

            if (Set.of("attributeName", "type", "attributes").contains(fieldName)) continue;

            JsonNode fieldSchema = metadataNode.get(fieldName);
            JsonNode actualValue = valueNode.get(fieldName);

            if (!fieldSchema.isObject()) continue;

            // Validate mandatory
            boolean isMandatory = fieldSchema.path("mandatory").asBoolean(false);
            if (isMandatory && (actualValue == null || actualValue.isNull())) {
                throw new InvalidInputException("Missing mandatory field '" + fieldName + "' in attribute '" + attrName + "'");
            }

            // Validate type
            String expectedType = fieldSchema.path("type").asText();
            if (actualValue != null && !actualValue.isNull()) {
                switch (expectedType.toLowerCase()) {
                    case "number" -> {
                        try {
                            Double.parseDouble(actualValue.asText());
                        } catch (NumberFormatException e) {
                            throw new InvalidInputException("Field '" + fieldName + "' in '" + attrName + "' is not a valid number: " + actualValue);
                        }
                    }
                    case "string" -> {
                        if (!actualValue.isTextual()) {
                            throw new InvalidInputException("Field '" + fieldName + "' in '" + attrName + "' is not a string: " + actualValue);
                        }
                    }
                    // Add other types as needed (e.g., boolean, date)
                }
            }

            // Validate validValues
            JsonNode validValues = fieldSchema.path("validValues");
            if (validValues.isArray() && validValues.size() > 0 && actualValue != null && !actualValue.isNull()) {
                boolean isValid = false;
                for (JsonNode valid : validValues) {
                    if (valid.asText().equals(actualValue.asText())) {
                        isValid = true;
                        break;
                    }
                }
                if (!isValid) {
                    throw new InvalidInputException("Invalid value '" + actualValue.asText() + "' for field '" + fieldName + "' in '" + attrName + "'");
                }
            }
        }
    }

    private static JsonNode buildFieldJsonNode(String fieldName, JsonNode attrNode) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode synthetic = mapper.createObjectNode();

        JsonNode fieldSchema = attrNode.get(fieldName);
        if (fieldSchema != null && fieldSchema.has("validValues") && fieldSchema.get("validValues").isArray()) {
            JsonNode first = fieldSchema.get("validValues").get(0);
            if (first != null && !first.isNull()) {
                synthetic.put(fieldName, first.asText());
                return synthetic;
            }
        }

        // fallback dummy
        synthetic.put(fieldName, "");
        return synthetic;
    }
}
