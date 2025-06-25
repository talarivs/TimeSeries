package com.example.demo;

import java.time.Instant;

public class Data {
    private int id;
    private String name;
    private String code;
    private Instant updatedDate;

    public Data() {
    }

    public Data(int id, String name, String code, Instant updatedDate) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.updatedDate = updatedDate;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCode() {
        return code;
    }

    public Instant getUpdatedDate() {
        return updatedDate;
    }
}
