package com.db_json.demo;

import com.db_json.demo.InvalidInputException;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeSeriesInputValidator {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesInputValidator.class);

    public static void validate(JsonNode input) {
        logger.info("Validating input JSON...");

        if (input == null || input.isEmpty()) {
            throw new InvalidInputException("Input JSON is empty or null.");
        }

        // Required top-level fields
        String[] requiredFields = {"entity", "entityType", "attributes"};
        for (String field : requiredFields) {
            if (!input.has(field)) {
                throw new InvalidInputException("Missing required field: " + field);
            }
        }

        // Required arrays
        String[] requiredArrays = {"timeBucketKey", "timeBucket"};
        for (String arrayField : requiredArrays) {
            if (!input.has(arrayField) || !input.get(arrayField).isArray() || input.get(arrayField).isEmpty()) {
                throw new InvalidInputException("Missing or empty array field: " + arrayField);
            }
        }

        // Validate attributes
        for (JsonNode attr : input.get("attributes")) {
            if (!attr.has("attributeName") || !attr.has("type")) {
                throw new InvalidInputException("Each attribute must have 'attributeName' and 'type'");
            }
        }

        // If everything passes, log input parameters
        logger.info("Valid input received:");
        logger.info("entity: {}", input.get("entity").asText());
        logger.info("entityType: {}", input.get("entityType").asText());
        logger.info("timeBucketKey: {}", input.get("timeBucketKey").toString());
        logger.info("timeBucket: {}", input.get("timeBucket").toString());
        logger.info("attributes: {}", input.get("attributes").toString());

        if (input.has("date")) {
            logger.info("date: {}", input.get("date").asText());
        }
        if (input.has("year")) {
            logger.info("year: {}", input.get("year").asText());
        }
        if (input.has("time_bucket")) {
            logger.info("time_bucket: {}", input.get("time_bucket").asText());
        }

        logger.info("Input validation passed successfully.");
    }
}
