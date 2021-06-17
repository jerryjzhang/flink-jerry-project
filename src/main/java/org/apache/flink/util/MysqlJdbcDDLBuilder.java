package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;

public class MysqlJdbcDDLBuilder implements DDLBuilder {
    private final String db_host;
    private final String db_port;
    private final String db_username;
    private final String db_password;

    public MysqlJdbcDDLBuilder(String host, String port, String username, String password) {
        this.db_host = host;
        this.db_port = port;
        this.db_password = password;
        this.db_username = username;
    }

    @Override
    public String getDDLTableName(String database, String table) {
        return database + "." + table + "_db";
    }

    @Override
    public String getDDLString(String database, String table,
                               String columnDef, Configuration options) {
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true" +
                        "&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC",
                db_host, db_port, database);
        Integer sinkParallelism = options.get(DDLBuilder.OPTION_SINK_PARAMETER);
        return String.format(
                "CREATE TABLE %s (\n" +
                        " %s) WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = '%s',\n" +
                        " 'username' = '%s',\n" +
                        " 'password' = '%s',\n" +
                        " 'table-name' = '%s',\n" +
                        " 'sink.buffer-flush.max-rows' = '10000',\n" +
                        " 'sink.buffer-flush.interval' = '60s',\n" +
                        " 'sink.parallelism' = '%d'\n" +
                        ")",
                getDDLTableName(database, table), columnDef, jdbcUrl,
                db_username, db_password, table, sinkParallelism);
    }
}
