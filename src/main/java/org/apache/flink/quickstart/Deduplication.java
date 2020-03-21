package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class Deduplication {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 1),
                new Order(2L, "pen", 1),
                new Order(4L, "beer", 3)));

        // register DataStream as Table
        tEnv.createTemporaryView("OrderB", orderB, "user, product, amount, ts, proctime.proctime");

        // union the two tables
        Table result = tEnv.sqlQuery("" +
                "SELECT user,product,amount,ts FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (PARTITION BY user,product ORDER BY proctime ASC) as row_num" +
                "   FROM OrderB)" +
                "WHERE row_num = 1");

        tEnv.toAppendStream(result, Order.class).printToErr();

        env.execute();
    }
}
