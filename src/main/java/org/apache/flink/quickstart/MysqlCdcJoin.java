package org.apache.flink.quickstart;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Random;
import java.util.TimeZone;

import static org.apache.flink.table.api.Expressions.$;

public class MysqlCdcJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        String cdcDDL = String.format(
                "CREATE TABLE dimTable (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " update_time TIMESTAMP(3)," +
                        " PRIMARY KEY (id) NOT ENFORCED," +
                        " WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        " 'connector' = 'mysql-cdc'," +
                        " 'hostname' = '%s'," +
                        " 'port' = '%s'," +
                        " 'username' = '%s'," +
                        " 'password' = '%s'," +
                        " 'database-name' = '%s'," +
                        " 'table-name' = '%s'" +
                        ")",
                "localhost", 3306, "jerryjzhang", "tme", "jerry", "products");
        String sinkDDL = "CREATE TABLE sinkTable (" +
                " a INT," +
                " b INT," +
                " t TIMESTAMP(9)," +
                " name STRING," +
                " PRIMARY KEY (a) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.createTemporaryView("sourceTable", env.addSource(new RandomFibonacciSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple3<Integer, Integer, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                        .withTimestampAssigner((ctx) -> new TupleExtractor())),
                $("a"), $("b"), $("rt").rowtime());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(cdcDDL);

        tEnv.executeSql("insert into sinkTable " +
                "select T.a, T.b, T.rt, D.name " +
                "from sourceTable AS T " +
                "LEFT JOIN dimTable FOR SYSTEM_TIME AS OF T.rt AS D " +
                "ON T.a = D.id");

        //tEnv.executeSql("insert into sinkTable select T.*, 'jerry' from sourceTable as T");
    }

    /**
     * Generate BOUND number of random integer pairs from the range from 1 to
     * BOUND/2.
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple3<Integer, Integer, Timestamp>> {
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
            while (isRunning && counter < BOUND) {
                int first = rnd.nextInt(10) + 101;
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;

                long time = (sdf.parse("2021-06-05 15:00:00.000").getTime());
                ctx.collect(new Tuple3<>(first, second, new Timestamp(time)));
                counter++;
                Thread.sleep(500L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    static class TupleExtractor implements TimestampAssigner<Tuple3<Integer, Integer, Timestamp>> {
        @Override
        public long extractTimestamp(Tuple3<Integer, Integer, Timestamp> element, long recordTimestamp) {
                return element.f2.getTime();
        }
    }
}