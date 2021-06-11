package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Order;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class Deduplication {
    static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 1,
                        new Timestamp(sdf.parse("2020-10-23").getTime())),
                new Order(2L, "pen", 2,
                        new Timestamp(sdf.parse("2020-10-23").getTime()+3600)),
                new Order(2L, "pen", 2,
                        new Timestamp(sdf.parse("2020-10-23").getTime()+7200)),
                new Order(2L, "pen", 2,
                        new Timestamp(sdf.parse("2020-10-23").getTime()+2400)),
                new Order(2L, "pen", 4,
                        new Timestamp(sdf.parse("2020-10-24").getTime())),
                new Order(4L, "beer", 3)));

        // register DataStream as Table
        tEnv.createTemporaryView("OrderB", orderB, "user, product, amount, ts, proctime.proctime");

        // union the two tables
        Table result = tEnv.sqlQuery("" +
                "SELECT amount,product,ts,user FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (PARTITION BY user,product,DATE_FORMAT(ts, 'yyMMdd') " +
                "ORDER BY proctime ASC) as row_num" +
                "   FROM OrderB)" +
                "WHERE row_num <= 3");

        tEnv.toRetractStream(result, Order.class).printToErr();

        env.execute();
    }
}
