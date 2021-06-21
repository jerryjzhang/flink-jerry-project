package org.apache.flink.cdc.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.cdc.ddl.ColumnDDLBuilder;
import org.apache.flink.cdc.ddl.EsTableDDLBuilder;
import org.apache.flink.cdc.ddl.KafkaTableDDLBuilder;

import java.util.List;

public class Kafka2EsJob extends AbstractJob {
    private final String kafkaServers;
    private final String kafkaGroupId;
    private final String esHost;
    private final String esIndex;
    private final String esIndexId;
    private final ColumnDDLBuilder columnBuilder;

    public Kafka2EsJob(String kafkaServers, String kafkaGroupId,
                       String esHost, String esIndex, String esIndexId,
                       ColumnDDLBuilder columnBuilder) {
        this.kafkaServers = kafkaServers;
        this.kafkaGroupId = kafkaGroupId;
        this.esHost = esHost;
        this.esIndex = esIndex;
        this.esIndexId = esIndexId;
        this.columnBuilder = columnBuilder;
    }

    public void addSync(String sourceDatabase, String sourceTable,
                         String sinkDatabase, String sinkTable,
                        List<String> includeColumns)throws Exception {
        // Step 1: create database if needed
        tEnv.executeSql("create database if not exists " + sourceDatabase);
        tEnv.executeSql("create database if not exists " + sinkDatabase);

        // Step 2: generate column DDL
        String columnDDL = columnBuilder.getDDLString(sinkDatabase, sinkTable,
                new ColumnDDLBuilder.DDLContext().keyCol(esIndexId).includedColumns(includeColumns));

        // Step 3: create source table
        KafkaTableDDLBuilder kafkaTableDDLBuilder = new KafkaTableDDLBuilder(kafkaServers);
        String sourceDDL = kafkaTableDDLBuilder.getDDLString(sourceDatabase, sourceTable, columnDDL, new Configuration().set(KafkaTableDDLBuilder.OPTION_CONSUMER_GROUP_ID, kafkaGroupId));
        tEnv.executeSql(sourceDDL);
        System.out.println(sourceDDL);
        String sourceTableName = kafkaTableDDLBuilder.getDDLTableName(sourceDatabase, sourceTable);

        // Step 4: create sink table
        EsTableDDLBuilder esDDLBuilder = new EsTableDDLBuilder(esHost, esIndex);
        String sinkDDL = esDDLBuilder.getDDLString(sinkDatabase, sinkTable, columnDDL, new Configuration());
        tEnv.executeSql(sinkDDL);
        System.out.println(sinkDDL);
        String sinkTableName = esDDLBuilder.getDDLTableName(sinkDatabase, sinkTable);

        // Step 5: create insert sql
        String insert = String.format("INSERT INTO %s SELECT * FROM %s",
                sinkTableName, sourceTableName);
        System.out.println(insert);
        syncStatementSet.addInsertSql(insert);
    }
}
