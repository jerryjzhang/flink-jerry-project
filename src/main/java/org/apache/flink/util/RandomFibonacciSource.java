package org.apache.flink.util;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.TimeZone;

public class RandomFibonacciSource implements SourceFunction<Tuple3<Integer, Integer, Timestamp>>{
    /**
     * Generate BOUND number of random integer pairs from the range from 1 to
     * BOUND/2.
     */
    private static final int BOUND = 100;

    private static final long serialVersionUID = 1L;

    private Random rnd = new Random();

    private volatile boolean isRunning = true;
    private int counter = 0;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static {
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Timestamp>> ctx) throws Exception {
        long epoch = 1 * 1000;
        while (isRunning) {

            int first = rnd.nextInt(9) + 101;
            int second = rnd.nextInt(BOUND / 2 - 1) + 1;

            long time = (sdf.parse("2021-06-05 15:00:00.000").getTime()) + counter * epoch;
            ctx.collect(new Tuple3<>(first, second, new Timestamp(time)));
            counter++;
            Thread.sleep(500L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static class TupleExtractor implements TimestampAssigner<Tuple3<Integer, Integer, Timestamp>> {
        @Override
        public long extractTimestamp(Tuple3<Integer, Integer, Timestamp> element, long recordTimestamp) {
            return element.f2.getTime();
        }
    }
}
