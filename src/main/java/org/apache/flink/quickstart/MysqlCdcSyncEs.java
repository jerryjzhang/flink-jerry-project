package org.apache.flink.quickstart;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlDDLBuilder;

import java.util.Arrays;
import java.util.List;

public class MysqlCdcSyncEs {
    public static void main(String [] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        StatementSet statementSet = tEnv.createStatementSet();

        env.enableCheckpointing(10 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////tmp/checkpoints");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        MysqlDDLBuilder builder = new MysqlDDLBuilder("localhost", "3306",
                "jerryjzhang", "tme");
        String sourceDDL = builder.getCdcTableDDL("jerry", "products");
        String source2DDL = builder.getCdcTableDDL("jerry", "products_sink");

        String esDDLTemplate = "CREATE TABLE %s (\n" +
                "%s\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'jerry_product'\n" +
                ")";

        createTableSync(tEnv, statementSet, builder, esDDLTemplate,
                "jerry", "products", "id", Arrays.asList("id", "name"));
        createTableSync(tEnv, statementSet, builder, esDDLTemplate,
                "jerry", "products_sink", "id", Arrays.asList("id", "weight"));

        statementSet.execute();
    }

    public static void createTableSync(StreamTableEnvironment tEnv, StatementSet sqlInserts, MysqlDDLBuilder tableFactory,
                                String sinkDDLTemplate, String database, String table,
                                String keyColumn, List<String> selectColumns) throws Exception {
        String sourceTableName = database + "." + table;
        String sinkTableName = sourceTableName + "_es";

        String sourceDDL = tableFactory.getCdcTableDDL(database, table);
        String columnDef = tableFactory.getColumnDef(database, table,
                new MysqlDDLBuilder.DDLContext().keyCol(keyColumn), selectColumns);
        String sinkDDL = String.format(sinkDDLTemplate, sinkTableName, columnDef);

        tEnv.executeSql("create database if not exists " + database);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        System.out.println(sourceDDL);
        System.out.println(sinkDDL);

        String selects = String.join(",", selectColumns);
        String dml = String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s",
                sinkTableName, selects, selects, sourceTableName);
        sqlInserts.addInsertSql(dml);
        System.out.println(dml);
    }
}
