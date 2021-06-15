package org.apache.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlDDLBuilder;

public class MysqlCdcSyncKafka {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        MysqlDDLBuilder builder = new MysqlDDLBuilder("localhost", "3306",
                "jerryjzhang", "tme");
        String sourceDDL = builder.getCdcTableDDL("jerry", "products");
        String columnDef = builder.getColumnDef("jerry", "products",
                new MysqlDDLBuilder.DDLContext().keyCol("id"));
        String sinkDDL = String.format("CREATE TABLE jerry.products_kafka (\n" +
                " %s" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'c2_track',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'debezium-json'\n" +
                ")", columnDef);

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jerry");
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);

        tEnv.executeSql("INSERT INTO jerry.products_kafka SELECT * FROM jerry.products");
    }
}
