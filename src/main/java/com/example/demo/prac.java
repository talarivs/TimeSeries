package com.example.demo;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.Connection;

public class prac {
    public static void main(String[] args){
        String url = "jdbc:postgresql://localhost:5432/spricedb";
        String username = "postgres";
        String password = "mysecretpassword";
        String Createtable = """
                    Create table if not exists id(
                        Emp_ID INTEGER,
                        Emp_Name TEXT
                    );
                """;
        String InsertValTab = """
                    Insert into id(Emp_ID, Emp_Name)
                    VALUES(?, ?)
                """;

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(InsertValTab)){
             stmt.execute(Createtable);

             int Emp_ID = 237858;
             String Emp_NAME = "SUNNY";
             pstmt.setInt(1, Emp_ID);
             pstmt.setString(2, Emp_NAME);
             pstmt.executeUpdate();
             System.out.println("yes");

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
