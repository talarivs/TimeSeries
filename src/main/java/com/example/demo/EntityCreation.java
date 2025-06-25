package com.example.demo;

import java.sql.*;
import java.time.ZonedDateTime;
import java.time.ZoneId;

public class EntityCreation {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/spricedb";
        String user = "postgres";
        String password = "mysecretpassword";

        String createTableSQL = "CREATE TABLE product_metrics (" +
                "productID TEXT, " +
                "region TEXT, " +
                "timeseries_bucket_quaterly TIMESTAMPTZ[], " +
                "timeseries_bucket_annual TIMESTAMPTZ[] " +
                ")";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            stmt.execute(createTableSQL);
            System.out.println("created successfully");

            String insertSQL = "INSERT INTO product_metrics (productID, region, timeseries_bucket_quaterly, timeseries_bucket_annual) VALUES (?, ?, ?::timestamptz[],  ?::timestamptz[])";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

                Timestamp quarterly = Timestamp.from(ZonedDateTime.of(2025, 3, 31, 0, 0, 0, 0, ZoneId.of("Asia/Kolkata")).toInstant());
                Timestamp annual = Timestamp.from(ZonedDateTime.of(2025, 12, 31, 0, 0, 0, 0, ZoneId.of("Asia/Kolkata")).toInstant());

                Timestamp[] timeseriesArray = new Timestamp[]{quarterly};
                Timestamp[] timeseriesArray1 = new Timestamp[]{annual};

                pstmt.setString(1, "123");
                pstmt.setString(2, "India");
                pstmt.setArray(3, conn.createArrayOf("timestamptz", timeseriesArray));
                pstmt.setArray(4, conn.createArrayOf("timestamptz", timeseriesArray1));


                pstmt.executeUpdate();
                System.out.println("Inserted quarterly and annual timestamps.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
