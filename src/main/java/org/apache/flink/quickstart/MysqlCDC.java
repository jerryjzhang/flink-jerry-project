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

        String sinkEsDDL = "CREATE TABLE product_es (\n" +
                "name STRING,\n" +
                "weight DECIMAL(10,3),\n" +
                "PRIMARY KEY(name) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'jerry_product'\n" +
                ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(sinkEsDDL);

        tEnv.executeSql("INSERT INTO product_es SELECT name, weight FROM " +
                "product_source");
        tEnv.executeSql("INSERT INTO product_sink SELECT name, weight FROM " +
                "product_source");

        env.execute("CDC demo");
    }
}
