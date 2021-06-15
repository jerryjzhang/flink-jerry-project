package org.apache.flink.cdc;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlDDLBuilder;

public class MysqlKafkaSyncJdbc {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        MysqlDDLBuilder builder = new MysqlDDLBuilder("localhost", "3306",
                "jerryjzhang", "tme");
        String sinkDDL = builder.getJdbcTableDDL("jerry", "products_sink",
                new MysqlDDLBuilder.DDLContext().keyCol("id"));        String columnDef = builder.getColumnDef("jerry", "products",
                new MysqlDDLBuilder.DDLContext().keyCol("id"));
        String sourceDDL = String.format("CREATE TABLE jerry.products_kafka (\n" +
                " %s" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'c2_track',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'debezium-json'\n" +
                ")", columnDef);
        String printDDL = String.format("create table jerry.products_print (" +
                "%s " +
                ") WITH (" +
                " 'connector' = 'print'\n" +
                " )", columnDef);

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jerry");
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        tEnv.executeSql(printDDL);
        System.out.println(printDDL);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("INSERT INTO jerry.products_print SELECT * FROM jerry.products_kafka");
        tEnv.executeSql("INSERT INTO jerry.products_sink SELECT * FROM jerry.products_kafka");
    }
}
