package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;

public class KafkaDDLBuilder implements DDLBuilder {
    private final String kafkaServers;

    public KafkaDDLBuilder(String kafkaServers) {
        this.kafkaServers = kafkaServers;
    }

    @Override
    public String getDDLTableName(String database, String table) {
        return database + "." + table + "_kafka";
    }

    @Override
    public String getDDLString(String database, String table, String columnDef, Configuration options) {
        String tableName = getDDLTableName(database, table);
        String topic = database + "." + table;
        Integer sinkParallelism = options.get(DDLBuilder.OPTION_SINK_PARAMETER);
        return  String.format("CREATE TABLE %s (\n" +
                        " %s" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = '%s',\n" +
                        " 'properties.bootstrap.servers' = '%s',\n" +
                        " 'format' = 'debezium-json',\n" +
                        " 'properties.group.id' = 'cmsSyncGroup',\n" +
                        " 'scan.startup.mode' = 'earliest-offset',\n" +
                        " 'sink.parallelism' = '%d'\n" +
                        ")",
                tableName, columnDef, topic, kafkaServers, sinkParallelism);
    }
}
