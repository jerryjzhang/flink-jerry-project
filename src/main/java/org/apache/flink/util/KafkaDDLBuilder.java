package org.apache.flink.util;

public class KafkaDDLBuilder extends AbstractDDLBuilder {
    private final String kafkaServers;

    public KafkaDDLBuilder(String kafkaServers) {
        this.kafkaServers = kafkaServers;
    }

    @Override
    public String getDDLTableName(String database, String table) {
        return database + "." + table + "_kafka";
    }

    @Override
    public String getDDLString(String database, String table, DDLContext ctx) {
        String tableName = getDDLTableName(database, table);
        String topic = database + "." + table;
        return  String.format("CREATE TABLE %s (\n" +
                        " %s" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = '%s',\n" +
                        " 'properties.bootstrap.servers' = '%s',\n" +
                        " 'format' = 'debezium-json',\n" +
                        " 'properties.group.id' = 'cmsSyncGroup',\n" +
                        " 'scan.startup.mode' = 'earliest-offset'\n" +
                        ")",
                tableName, ctx.columnDef, topic, kafkaServers);
    }
}
