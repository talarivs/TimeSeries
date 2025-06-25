package com.example.demo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;

public class TimeSeries {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var input = env.fromElements(
                new Data(123, "sunny", "123456789", Instant.parse("2025-06-01T22:39:51.684123Z"))
        );

        input.addSink(JdbcSink.sink(
                "INSERT INTO timeseries_data (id, name, code, updated_date) VALUES (?, ?, ?, ?)",
                (JdbcStatementBuilder<Data>) (stmt, r) -> {
                    stmt.setInt(1, r.getId());
                    stmt.setString(2, r.getName());
                    stmt.setString(3, r.getCode());
                    stmt.setTimestamp(4, Timestamp.from(r.getUpdatedDate()));
                },
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/spricedb")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("postgres")
                        .withPassword("mysecretpassword")
                        .build()
        ));

        env.execute("Flink Time Series to PostgreSQL");
    }


}
