package org.apache.flink.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

//        env.enableCheckpointing(5 * 60 * 1000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:////data/home/jerryjzhang/checkpoints");
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public void cdc2kafka(String sourceDatabase, String sourceTable,
                          Integer sinkParallelism)throws Exception {
        validParameters(context.kafkaServers,
                context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);

        tEnv.executeSql("create database if not exists " + sourceDatabase);

        MysqlCdcDDLBuilder cdcDDLBuilder = new MysqlCdcDDLBuilder(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String columnDef = cdcDDLBuilder.getDDLColumnDef(sourceDatabase, sourceTable,
                new MysqlJdbcDDLBuilder.DDLContext());

        String sourceDDL = cdcDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                new AbstractDDLBuilder.DDLContext());
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = cdcDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        KafkaDDLBuilder kafkaDDLBuilder = new KafkaDDLBuilder(context.kafkaServers);
        String sinkDDL = kafkaDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                new AbstractDDLBuilder.DDLContext()
                        .columnDef(columnDef)
                        .parallelism(sinkParallelism));
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = kafkaDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        String insert = String.format("INSERT INTO %s SELECT * FROM %s",
                sinkTableName, sourceTableName);
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

        MysqlJdbcDDLBuilder ddlBuilder = new MysqlJdbcDDLBuilder(context.jdbcHost, context.jdbcPort,
                context.jdbcUsername, context.jdbcPassword);
        String columnDef = ddlBuilder.getDDLColumnDef(sinkDatabase, sinkTable,
                new MysqlJdbcDDLBuilder.DDLContext());

        KafkaDDLBuilder kafkaDDLBuilder = new KafkaDDLBuilder(context.kafkaServers);
        String sourceDDL = kafkaDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                new AbstractDDLBuilder.DDLContext().columnDef(columnDef));
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = kafkaDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        String sinkDDL = ddlBuilder.getDDLString(sinkDatabase, sinkTable,
                new MysqlJdbcDDLBuilder.DDLContext().parallelism(sinkParallelism));
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = ddlBuilder.getDDLTableName(sinkDatabase, sinkTable);

        String insert = String.format("INSERT INTO %s SELECT * FROM %s",
                sinkTableName, sourceTableName);
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
    }

    private void validParameters(String... parameters) {
        for (String p : parameters) {
            assert p != null;
        }
    }
}
