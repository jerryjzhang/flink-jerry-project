package org.apache.flink.cdc.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.cdc.ddl.CdcTableDDLBuilder;
import org.apache.flink.cdc.ddl.ColumnDDLBuilder;
import org.apache.flink.cdc.ddl.KafkaTableDDLBuilder;
import org.apache.flink.cdc.ddl.TableDDLBuilder;

public class Cdc2KafkaJob extends AbstractSyncJob {
    private final String cdc_db_host;
    private final String cdc_db_port;
    private final String cdc_db_username;
    private final String cdc_db_password;
    private final String kafkaServers;

    public Cdc2KafkaJob(String host, String port, String username, String password, String kafkaServers) {
        this.cdc_db_host = host;
        this.cdc_db_port = port;
        this.cdc_db_username = username;
        this.cdc_db_password = password;
        this.kafkaServers = kafkaServers;
    }

    public void addSync(String sourceDatabase, String sourceTable,
                        int sinkParallelism)throws Exception {
        // Step 1: create database if needed
        tEnv.executeSql("create database if not exists " + sourceDatabase);

        // Step 2: infer column definition
        ColumnDDLBuilder columnGenerator = new ColumnDDLBuilder(cdc_db_host, cdc_db_port,
                cdc_db_username, cdc_db_password);
        String columnDDL = columnGenerator.getDDLString(sourceDatabase, sourceTable,
                ColumnDDLBuilder.DDLContext.EMPTY);

        // Step 3: create source table
        CdcTableDDLBuilder cdcDDLBuilder = new CdcTableDDLBuilder(cdc_db_host, cdc_db_port,
                cdc_db_username, cdc_db_password);
        String sourceDDL = cdcDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                columnDDL, new Configuration());
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = cdcDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 4: create sink table
        KafkaTableDDLBuilder kafkaTableDDLBuilder = new KafkaTableDDLBuilder(kafkaServers);
        String sinkDDL = kafkaTableDDLBuilder.getDDLString(sourceDatabase, sourceTable,
                columnDDL, new Configuration().set(TableDDLBuilder.OPTION_SINK_PARAMETER, sinkParallelism));
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = kafkaTableDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 5: create insert sql
        String insert = String.format("INSERT INTO %s SELECT * FROM %s", sinkTableName, sourceTableName);
        System.out.println(insert);
        syncStatementSet.addInsertSql(insert);
    }
}
