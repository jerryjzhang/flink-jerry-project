package org.apache.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

public class MysqlCdcJoin {
    public static void main(String [] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        String cdcDDL = String.format(
                "CREATE TABLE dimTable (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
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
                " name STRING," +
                " PRIMARY KEY (a) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.createTemporaryView("sourceTable", env.addSource(new RandomFibonacciSource()), $("a"), $("b"), $("proctime").proctime());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(cdcDDL);

        tEnv.executeSql("insert into sinkTable " +
                "select T.a, T.b, D.name " +
                "from sourceTable AS T " +
                "LEFT JOIN dimTable FOR SYSTEM_TIME AS OF T.proctime AS D " +
                "ON T.a = D.id");
    }

    /** Generate BOUND number of random integer pairs from the range from 1 to BOUND/2. */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final int BOUND = 100;

        private static final long serialVersionUID = 1L;

        private Random rnd = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

            while (isRunning && counter < BOUND) {
                int first = rnd.nextInt(10) + 100;
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;

                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(500L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
