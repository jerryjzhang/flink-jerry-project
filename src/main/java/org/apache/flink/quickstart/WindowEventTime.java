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
        tEnv.registerDataStream("OrderB", orderB, "user, product, amount, ts1.rowtime");

        Table result = tEnv.sqlQuery("" +
                "SELECT TUMBLE_START(ts1, INTERVAL '5' MINUTE) as wStart, SUM(amount)" +
                "   FROM OrderB" +
                "   GROUP BY TUMBLE(ts1, INTERVAL '5' MINUTE)");

        tEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}
