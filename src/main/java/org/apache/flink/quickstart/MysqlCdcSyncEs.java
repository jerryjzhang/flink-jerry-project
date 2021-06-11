package org.apache.flink.quickstart;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlDDLBuilder;

public class MysqlCdcSyncEs {
    public static void main(String [] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        env.enableCheckpointing(10 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////tmp/checkpoints");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        MysqlDDLBuilder builder = new MysqlDDLBuilder("localhost", "3306",
                "jerryjzhang", "tme");
        String sourceDDL = builder.getCdcTableDDL("jerry", "products");
        String source2DDL = builder.getCdcTableDDL("jerry", "products_sink");

        String sinkEsDDL = "CREATE TABLE product_es (\n" +
                "id INT," +
                "name STRING,\n" +
                "PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'jerry_product'\n" +
                ")";

        String sinkEs2DDL = "CREATE TABLE product_es2 (\n" +
                "id INT," +
                "weight DECIMAL(10,3),\n" +
                "PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'jerry_product'\n" +
                ")";

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jerry");
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(source2DDL);
        tEnv.executeSql(sinkEs2DDL);
        tEnv.executeSql(sinkEsDDL);

        tEnv.executeSql("INSERT INTO product_es SELECT id, name FROM " +
                "jerry.products");
        tEnv.executeSql("INSERT INTO product_es2 SELECT id, weight FROM " +
                "jerry.products_sink");
    }
}
