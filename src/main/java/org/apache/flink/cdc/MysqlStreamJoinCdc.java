package org.apache.flink.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.RandomFibonacciSource;

import java.sql.Timestamp;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class MysqlStreamJoinCdc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        env.setParallelism(1);

        String cdcDDL = String.format(
                "CREATE TABLE dimTable (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " update_time TIMESTAMP(3)," +
                        " PRIMARY KEY (id) NOT ENFORCED," +
                        " WATERMARK FOR update_time AS update_time + INTERVAL '1' DAYS"  +
                        ") WITH (" +
                        " 'connector' = 'mysql-cdc'," +
                        " 'hostname' = '%s'," +
                        " 'port' = '%s'," +
                        " 'username' = '%s'," +
                        " 'password' = '%s'," +
                        " 'database-name' = '%s'," +
                        " 'table-name' = '%s'," +
                        " 'debezium.database.serverTimezone' = 'UTC'" +
                        ")",
                "localhost", 3306, "jerryjzhang", "tme", "jerry", "products");
        String sinkDDL = "CREATE TABLE sinkTable (" +
                " a INT," +
                " b INT," +
                " t TIMESTAMP(9)," +
                " name STRING," +
                " ut TIMESTAMP(9)," +
                " PRIMARY KEY (a) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.createTemporaryView("sourceTable", env.addSource(new RandomFibonacciSource()).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<Integer, Integer, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                 .withTimestampAssigner((ctx) -> new RandomFibonacciSource.TupleExtractor())),
                $("a"), $("b"), $("rt").rowtime(), $("pt").proctime());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(cdcDDL);

        tEnv.executeSql("insert into sinkTable " +
                "select T.a, T.b, T.rt, D.name, D.update_time " +
                "from sourceTable AS T " +
                "LEFT JOIN dimTable FOR SYSTEM_TIME AS OF T.rt AS D " +
                "ON T.a = D.id");
    }
}