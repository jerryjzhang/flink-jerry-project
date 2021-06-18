package org.apache.flink.cdc.ddl;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public class KafkaTableDDLBuilder implements TableDDLBuilder {
    public static final ConfigOption<String> OPTION_CONSUMER_GROUP_ID =
            ConfigOptions.key("kafka.group.id").defaultValue("cmsSyncGroup");
    private final String kafkaServers;

    public KafkaTableDDLBuilder(String kafkaServers) {
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
        Integer sinkParallelism = options.get(TableDDLBuilder.OPTION_SINK_PARAMETER);
        String  groupId = options.get(OPTION_CONSUMER_GROUP_ID);
        return  String.format("CREATE TABLE %s (\n" +
                        " %s" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = '%s',\n" +
                        " 'properties.bootstrap.servers' = '%s',\n" +
                        " 'format' = 'debezium-json',\n" +
                        " 'properties.group.id' = '%s',\n" +
                        " 'scan.startup.mode' = 'earliest-offset',\n" +
                        " 'sink.parallelism' = '%d'\n" +
                        ")",
                tableName, columnDef, topic, kafkaServers, groupId, sinkParallelism);
    }
}
