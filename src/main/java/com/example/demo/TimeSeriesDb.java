package com.example.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.apache.commons.codec.digest.DigestUtils;

public class TimeSeriesDb {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/spricedb";
        String user = "postgres";
        String password = "mysecretpassword";


        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS actual (
                ProductID_Region TEXT,
                TimeSeries JSONB
            );
        """;


        String insertSQL = """
            INSERT INTO actual (ProductID_Region, TimeSeries)
            VALUES (?, jsonb_build_array(jsonb_build_object(?, ?, ?, ?)))
        """;

        String indexCreation = """
            CREATE INDEX IF NOT EXISTS idx_actual_product_hash ON actual (ProductID_Region);
        """;

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {


            stmt.execute(createTableSQL);


            String productId = "123";
            String region = "India";
            String rawKey = productId + "_" + region;
            String hashedKey = DigestUtils.md5Hex(rawKey);
            String finalKey = "\"" + hashedKey + " : " + productId + " : " + region + "\"";


            pstmt.setString(1, finalKey);
            pstmt.setString(2, "2025 Q1");
            pstmt.setString(3, "2025-03-31T00:00:00+00:00");
            pstmt.setString(4, "2025 A1");
            pstmt.setString(5, "2025-12-31T00:00:00+00:00");


            pstmt.executeUpdate();
            System.out.println("Table created and data inserted successfully");


            stmt.execute(indexCreation);
            System.out.println("Index Created Successfully");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
