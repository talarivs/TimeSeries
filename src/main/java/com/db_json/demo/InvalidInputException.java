package com.db_json.demo;

/**
 * Custom exception thrown when input validation fails for time series data.
 */
public class InvalidInputException extends RuntimeException {

    public InvalidInputException(String message) {
        super(message);
    }
}
