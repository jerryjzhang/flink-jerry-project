package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;

public class WindowDemo {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long now = System.currentTimeMillis();
        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(1L, "pen", 1, new Timestamp(now)),
                new Order(1L, "rubber", 2, new Timestamp(now + 1*60*1000)),
                new Order(1L, "beer", 3, new Timestamp(now + 5*60*1000)),
                new Order(1L, "jesse", 4, new Timestamp(now + 6*60*1000))));

        // step1: extract event-time
        orderB = orderB.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.ts.getTime();
            }
        });


        DataStream<Tuple2<TimeWindow, Integer>> sum = orderB
                .keyBy(new KeySelector<Order, Long>(){
                    public Long getKey(Order value) throws Exception {
                        return value.user;
                    }})
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .trigger(CountTrigger.of(1))
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());

        sum.printToErr();

        env.execute();
    }

    private static class MyAggregateFunction implements AggregateFunction<Order, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return new Integer(0);
        }

        @Override
        public Integer add(Order value, Integer accumulator) {
            return accumulator + value.amount;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Integer, Tuple2<TimeWindow, Integer>, Long, TimeWindow> {
        @Override
        public void process(Long aLong, Context context, java.lang.Iterable<Integer> elements, Collector<Tuple2<TimeWindow, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(context.window() ,elements.iterator().next()));
        }
    }
}
