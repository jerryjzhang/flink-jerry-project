package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MyTimeWindow;

import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.util.Utils.getTime;

public class WindowDemo {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Order> orders = env.fromCollection(Arrays.asList(
                new Order(1L, "pen", 1,    getTime("2020-04-15 20:00")),
                new Order(1L, "rubber", 2, getTime("2020-04-15 20:01")),

                new Order(1L, "beer", 1,   getTime("2020-04-15 20:05")),
                new Order(1L, "ball", 3,   getTime("2020-04-15 20:06")),

                new Order(2L, "pen", 3,    getTime("2020-04-15 20:00")),
                new Order(2L, "rubber", 1, getTime("2020-04-15 20:01")),

                new Order(2L, "beer", 2,   getTime("2020-04-15 20:05")),
                new Order(2L, "ball", 1,   getTime("2020-04-15 20:06"))));

        DataStream<Tuple3<Long, MyTimeWindow, Integer>> result = orders
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(5)) {
                    public long extractTimestamp(Order element) {
                        return element.ts.getTime();
                     }})
                .keyBy(value -> value.user)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))

                .trigger(EventTimeTrigger.create())
                //.trigger(CountTrigger.of(1))
                //.trigger(ContinuousEventTimeTrigger.of(Time.minutes(2)))
                //.trigger(PurgingTrigger.of(CountTrigger.of(1)))

                //.evictor(CountEvictor.of(1))
                //.evictor(TimeEvictor.of(Time.minutes(1)))

                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());

        result.printToErr();

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
            extends ProcessWindowFunction<Integer, Tuple3<Long, MyTimeWindow, Integer>, Long, TimeWindow> {
        @Override
        public void process(Long userId, Context context, java.lang.Iterable<Integer> elements, Collector<Tuple3<Long, MyTimeWindow, Integer>> out) throws Exception {
            out.collect(new Tuple3<>(userId, new MyTimeWindow(context.window()) ,elements.iterator().next()));
        }
    }
}
