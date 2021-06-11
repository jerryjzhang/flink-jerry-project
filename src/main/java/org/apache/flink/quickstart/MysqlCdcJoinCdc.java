package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCdcJoinCdc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        env.setParallelism(1);

        String sourceDDL = String.format(
                "CREATE TABLE table1 (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " update_time TIMESTAMP(3)," +
                        " WATERMARK FOR update_time AS update_time - INTERVAL '5' SECONDS"  +
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
        String source2DDL = String.format(
                "CREATE TABLE table2 (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " update_time TIMESTAMP(3)," +
                        " PRIMARY KEY (id) NOT ENFORCED," +
                        " WATERMARK FOR update_time AS update_time"  +
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
                "localhost", 3306, "jerryjzhang", "tme", "jerry", "products_sink");
        String sinkDDL = "CREATE TABLE sinkTable (" +
                " a INT," +
                " b INT," +
                " name STRING," +
                " ut TIMESTAMP(9)," +
                " PRIMARY KEY (a) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(source2DDL);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("insert into sinkTable " +
                "select T.id, D.id, T.name, T.update_time " +
                "from table1 AS T " +
                "LEFT JOIN table2 FOR SYSTEM_TIME AS OF T.update_time AS D " +
                "ON T.id = D.id");
    }
}