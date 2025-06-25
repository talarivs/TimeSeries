package com.example.demo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class practice {
    public static void main(String[] args){
        String url = "jdbc:postgresql://localhost:5432/spricedb";
        String username = "postgres";
        String password = "mysecretpassword";

        String CreateTable = """ 
                Create table if not exists sunny (
                    first_name TEXT,
                    second_name TEXT
                );
             """;
        String InsertValues = """
                    insert into sunny(first_name, second_name)
                    VALUES (?, ?)
                """;
        try (Connection conn = DriverManager.getConnection(url, username, password);
                Statement stm = conn.createStatement();
                PreparedStatement pstm = conn.prepareStatement(InsertValues)){
            stm.execute(CreateTable);

            String first_name = "talari";
            String second_name = "venkata";
            pstm.setString(1, first_name);
            pstm.setString(2, second_name);
            pstm.executeUpdate();
            System.out.println("ok");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
