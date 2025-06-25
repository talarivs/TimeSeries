package com.example.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class TimeSeriesJson {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/spricedb";
        String user = "postgres";
        String password = "mysecretpassword";

        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS product_metrics (
                entityType JSONB,
                attributeName JSONB
            );
        """;

        String insertSQL = """
            INSERT INTO product_metrics (entityType, attributeName)
                    VALUES (
                        to_jsonb(?::text),
                        jsonb_build_object(
                            'attr1', jsonb_build_array(
                                jsonb_build_object(
                                    'name', '',
                                    'weight', '',
                                    'score_value', '',
                                    'score', ''
                                )
                            ),
                            'attr2', jsonb_build_object(
                                'name', '',
                                'weight', '',
                                'score_value', '',
                                'score', ''
                            ),
                            'attr3', '',
                            'attr4', ''
                        )
                    );
        """;

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement createStmt = conn.createStatement();
             PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {


            createStmt.execute(createTableSQL);


            insertStmt.setString(1, "timeSeries");
            insertStmt.executeUpdate();

            System.out.println("JSON Created");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
