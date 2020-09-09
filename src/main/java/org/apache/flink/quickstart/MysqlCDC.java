package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCDC {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        String sourceDDL = String.format(
                "CREATE TABLE product_source (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)" +
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

        String sinkDDL = "CREATE TABLE product_sink (" +
                " name STRING," +
                " weight DECIMAL(10,3)," +
                " PRIMARY KEY (name) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("INSERT INTO product_sink SELECT name, weight FROM " +
                "product_source");

        env.execute("CDC demo");
    }
}
