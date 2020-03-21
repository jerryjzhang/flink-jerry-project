package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Arrays;

public class WindowEventTime {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long now = System.currentTimeMillis();
        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(1L, "pen", 1, new Timestamp(now)),
                new Order(2L, "rubber", 2, new Timestamp(now + 1*60*1000)),
                new Order(3L, "beer", 3, new Timestamp(now + 5*60*1000)),
                new Order(4L, "jesse", 3, new Timestamp(now + 6*60*1000))));
        orderB = orderB.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.ts.getTime();
            }
        });

        // register DataStream as Table
        tEnv.createTemporaryView("OrderB", orderB, "user, product, amount, ts1.rowtime");

        Table result = tEnv.sqlQuery("" +
                "SELECT TUMBLE_START(ts1, INTERVAL '5' MINUTE) as window_start, SUM(amount) as total_amount" +
                "   FROM OrderB" +
                "   GROUP BY TUMBLE(ts1, INTERVAL '5' MINUTE)");

        tEnv.toAppendStream(result, Row.class).print();

        result.printSchema();
        tEnv.createTemporaryView("OrderStat", result);
        Table result1 = tEnv.sqlQuery("" +
                "SELECT window_start, total_amount FROM ("+
                "       SELECT *, ROW_NUMBER() OVER (ORDER BY total_amount DESC) as row_num" +
                "       FROM OrderStat)" +
                "WHERE row_num<=1");
        tEnv.toRetractStream(result1, Row.class).print();

        env.execute();
    }
}
