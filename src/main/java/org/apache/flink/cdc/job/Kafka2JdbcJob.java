package org.apache.flink.cdc.job;

import org.apache.flink.cdc.ddl.*;
import org.apache.flink.configuration.Configuration;

public class Kafka2JdbcJob extends AbstractJob {
    private final String jdbcHost;
    private final String jdbcPort;
    private final String jdbcUsername;
    private final String jdbcPassword;
    private final String kafkaServers;
    private final String kafkaGroupId;

    public Kafka2JdbcJob(String host, String port, String username, String password,
                         String kafkaServers, String kafkaGroupId) {
        this.jdbcHost = host;
        this.jdbcPort = port;
        this.jdbcUsername = username;
        this.jdbcPassword = password;
        this.kafkaServers = kafkaServers;
        this.kafkaGroupId = kafkaGroupId;
    }

    public void addSync(String sourceDatabase, String sourceTable,
                        String sinkDatabase, String sinkTable,
                        int sinkParallelism)throws Exception {
        // Step 1: create database if needed
        tEnv.executeSql("create database if not exists " + sourceDatabase);
        tEnv.executeSql("create database if not exists " + sinkDatabase);

        // Step 2: infer column definition
        ColumnDDLBuilder columnGenerator = new ColumnDDLBuilder(jdbcHost, jdbcPort,
                jdbcUsername, jdbcPassword);
        String columDDL = columnGenerator.getDDLString(sinkDatabase, sinkTable,
                ColumnDDLBuilder.DDLContext.EMPTY);

        // Step 3: create source table
        KafkaTableDDLBuilder kafkaTableDDLBuilder = new KafkaTableDDLBuilder(kafkaServers);
        String sourceDDL = kafkaTableDDLBuilder.getDDLString(sourceDatabase, sourceTable, columDDL,
                new Configuration().set(KafkaTableDDLBuilder.OPTION_CONSUMER_GROUP_ID, kafkaGroupId));
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = kafkaTableDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 4: create sink table
        JdbcTableDDLBuilder jdbcDDLBuilder = new JdbcTableDDLBuilder(jdbcHost, jdbcPort,
                jdbcUsername, jdbcPassword);
        String sinkDDL = jdbcDDLBuilder.getDDLString(sinkDatabase, sinkTable, columDDL,
                new Configuration().set(TableDDLBuilder.OPTION_SINK_PARAMETER, sinkParallelism));
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = jdbcDDLBuilder.getDDLTableName(sinkDatabase, sinkTable);

        // Step 5: create insert sql
        String insert = String.format("INSERT INTO %s SELECT * FROM %s",
                sinkTableName, sourceTableName);
        System.out.println(insert);
        syncStatementSet.addInsertSql(insert);
    }
}
