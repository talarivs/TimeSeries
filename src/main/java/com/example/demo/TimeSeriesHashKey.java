package com.example.demo;

import org.apache.commons.codec.digest.DigestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class TimeSeriesHashKey {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/spricedb";
        String user = "postgres";
        String password = "mysecretpassword";


        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS product_timeseries (
                raw_info TEXT PRIMARY KEY,
                hash_key TEXT UNIQUE,
                time_key TEXT,
                time_hash TEXT UNIQUE,
                timeseries JSONB
            );
        """;


        String insertSQL = """
            INSERT INTO product_timeseries (raw_info, hash_key, time_key, time_hash, timeseries)
            VALUES (?, ?, ?, ?, ?::jsonb)
        """;

        
        String indexSQL = """
            CREATE INDEX IF NOT EXISTS idx_hash_key ON product_timeseries(hash_key);
            CREATE INDEX IF NOT EXISTS idx_time_hash ON product_timeseries(time_hash);
        """;

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

            // Step 1: Create table and indexes
            stmt.execute(createTableSQL);
            stmt.execute(indexSQL);

            // Step 2: Define values
            String productId = "123";
            String region = "India";
            String year = "2024";
            String timeKey = "2024 Q1";

            String rawInfo = productId + ":" + region + ":" + year;
            String hashKey = DigestUtils.md5Hex(rawInfo);
            String timeHash = DigestUtils.md5Hex(timeKey);

            // Step 3: JSON TimeSeries data
            String jsonTimeSeries = """
                {
                  "2024 Q1": "2024-03-31T00:00:00+00:00",
                  "2024 A1": "2024-12-31T00:00:00+00:00"
                }
            """;

            // Step 4: Set parameters and insert
            pstmt.setString(1, rawInfo);
            pstmt.setString(2, hashKey);
            pstmt.setString(3, timeKey);
            pstmt.setString(4, timeHash);
            pstmt.setString(5, jsonTimeSeries);

            pstmt.executeUpdate();
            System.out.println("Data inserted successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
