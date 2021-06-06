package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PostgresCatalog {
    public static void main(String [] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        String sourceDDL = String.format(
                "CREATE CATALOG mypg WITH(\n" +
                "    'type' = 'jdbc',\n" +
                "    'default-database' = 'postgres',\n" +
                "    'username' = 'postgres',\n" +
                "    'password' = 'tme',\n" +
                "    'base-url' = 'jdbc:postgresql://localhost:5432'\n" +
                ")");

        String sinkDDL = "CREATE TABLE product_print (" +
                " id INT," +
                " name STRING," +
                " description STRING," +
                " weight DECIMAL(10,3)," +
                " PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("insert into mypg.postgres.products_sink select * from mypg.postgres.products");
        tEnv.executeSql("insert into product_print select * from mypg.postgres.products");
    }
}
