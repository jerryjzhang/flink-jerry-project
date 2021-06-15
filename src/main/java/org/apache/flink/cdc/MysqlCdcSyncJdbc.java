package org.apache.flink.cdc;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlDDLBuilder;

public class MysqlCdcSyncJdbc {
    public static void main(String [] args) throws Exception {
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
        String sinkDDL = builder.getJdbcTableDDL("jerry", "products_sink",
                new MysqlDDLBuilder.DDLContext().keyCol("id"));

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jerry");
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);

        tEnv.executeSql("INSERT INTO jerry.products_sink SELECT * FROM jerry.products");
    }
}
