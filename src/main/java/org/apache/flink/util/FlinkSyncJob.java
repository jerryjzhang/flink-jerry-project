package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

public class FlinkSyncJob {
    private final StatementSet syncStatementSet;
    private final StreamExecutionEnvironment env;
    private final StreamTableEnvironment tEnv;
    private final SyncContext context;

    public FlinkSyncJob(SyncContext ctx) {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        syncStatementSet = tEnv.createStatementSet();
        context = ctx;

        env.enableCheckpointing(5 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////data/home/jerryjzhang/checkpoints");
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public void cdc2kafka(String sourceDatabase, String sourceTable,
                          Integer sinkParallelism)throws Exception {
        validParameters(context.kafkaServers,
                context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);

        // Step 1: create database if needed
        tEnv.executeSql("create database if not exists " + sourceDatabase);

        // Step 2: infer column definition
        TableColumnGenerator columnGenerator = new TableColumnGenerator(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String columnDef = columnGenerator.generateDefinition(sourceDatabase, sourceTable,
                TableColumnGenerator.DDLContext.EMPTY);

        // Step 3: create source table
        MysqlCdcDDLBuilder cdcDDLBuilder = new MysqlCdcDDLBuilder(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String sourceDDL = cdcDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                columnDef, new Configuration());
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = cdcDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 4: create sink table
        KafkaDDLBuilder kafkaDDLBuilder = new KafkaDDLBuilder(context.kafkaServers);
        String sinkDDL = kafkaDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                columnDef, new Configuration().set(DDLBuilder.OPTION_SINK_PARAMETER, sinkParallelism));
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = kafkaDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 5: create insert sql
        String insert = String.format("INSERT INTO %s SELECT * FROM %s", sinkTableName, sourceTableName);
        System.out.println(insert);
        syncStatementSet.addInsertSql(insert);
    }

    public void kafka2Es(String sourceDatabase, String sourceTable,
                         String sinkDatabase, String sinkTable,
                         Integer sinkParallelism, List<String> includedColumns)throws Exception {
        validParameters(context.kafkaServers, context.esHost, context.esIndex,
                context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);

        // Step 1: create database if needed
        tEnv.executeSql("create database if not exists " + sourceDatabase);
        tEnv.executeSql("create database if not exists " + sinkDatabase);

        // Step 2: infer column definition
        TableColumnGenerator columnGenerator = new TableColumnGenerator(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String columnDef = columnGenerator.generateDefinition(sourceDatabase, sourceTable,
                new TableColumnGenerator.DDLContext().keyCol("Ftrack_id").includedColumns(includedColumns));

        // Step 3: create source table
        KafkaDDLBuilder kafkaDDLBuilder = new KafkaDDLBuilder(context.kafkaServers);
        String sourceDDL = kafkaDDLBuilder.getDDLString(sourceDatabase, sourceTable, columnDef, new Configuration());
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = kafkaDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 4: create sink table
        ElasticsearchDDLBuilder esDDLBuilder = new ElasticsearchDDLBuilder(context.esHost, context.esIndex);
        String sinkDDL = esDDLBuilder.getDDLString(sinkDatabase, sinkTable, columnDef, new Configuration());
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = esDDLBuilder.getDDLTableName(sinkDatabase, sinkTable);

        // Step 5: create insert sql
        String insert = String.format("INSERT INTO %s SELECT * FROM %s",
                sinkTableName, sourceTableName);
        System.out.println(insert);
        syncStatementSet.addInsertSql(insert);
    }

    public void kafka2Jdbc(String sourceDatabase, String sourceTable,
                           String sinkDatabase, String sinkTable,
                           Integer sinkParallelism)throws Exception {
        validParameters(context.kafkaServers,
                context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);

        tEnv.executeSql("create database if not exists " + sourceDatabase);
        tEnv.executeSql("create database if not exists " + sinkDatabase);

        // Step 1: create database if needed
        tEnv.executeSql("create database if not exists " + sourceDatabase);
        tEnv.executeSql("create database if not exists " + sinkDatabase);

        // Step 2: infer column definition
        TableColumnGenerator columnGenerator = new TableColumnGenerator(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String columnDef = columnGenerator.generateDefinition(sourceDatabase, sourceTable,
                TableColumnGenerator.DDLContext.EMPTY);

        // Step 3: create source table
        KafkaDDLBuilder kafkaDDLBuilder = new KafkaDDLBuilder(context.kafkaServers);
        String sourceDDL = kafkaDDLBuilder.getDDLString(sourceDatabase, sourceTable, columnDef,
                new Configuration());
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = kafkaDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 4: create sink table
        MysqlJdbcDDLBuilder jdbcDDLBuilder = new MysqlJdbcDDLBuilder(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String sinkDDL = jdbcDDLBuilder.getDDLString(sinkDatabase, sinkTable, columnDef,
                new Configuration().set(DDLBuilder.OPTION_SINK_PARAMETER, sinkParallelism));
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = jdbcDDLBuilder.getDDLTableName(sinkDatabase, sinkTable);

        // Step 5: create insert sql
        String insert = String.format("INSERT INTO %s SELECT * FROM %s",
                sinkTableName, sourceTableName);
        System.out.println(insert);
        syncStatementSet.addInsertSql(insert);
    }

    public void run() {
        syncStatementSet.execute();
    }

    public static class SyncContext {
        public String kafkaServers;
        public String kafkaGroupId;
        public String jdbcHost;
        public String jdbcPort;
        public String jdbcUsername;
        public String jdbcPassword;
        public String esHost;
        public String esIndex;

        public SyncContext kafkaServers(String kafkaServers) {
            this.kafkaServers = kafkaServers;
            return this;
        }
        public SyncContext kafkaGroupId(String kafkaGroupId) {
            this.kafkaGroupId = kafkaGroupId;
            return this;
        }
        public SyncContext jdbcHost(String jdbcHost) {
            this.jdbcHost = jdbcHost;
            return this;
        }
        public SyncContext jdbcPort(String jdbcPort) {
            this.jdbcPort = jdbcPort;
            return this;
        }
        public SyncContext jdbcUsername(String jdbcUsername) {
            this.jdbcUsername = jdbcUsername;
            return this;
        }
        public SyncContext jdbcPassword(String jdbcPassword) {
            this.jdbcPassword = jdbcPassword;
            return this;
        }
        public SyncContext esHost(String esHost) {
            this.esHost = esHost;
            return this;
        }
        public SyncContext esIndex(String esIndex) {
            this.esIndex = esIndex;
            return this;
        }
    }

    private void validParameters(String... parameters) {
        for (String p : parameters) {
            assert p != null;
        }
    }
}
