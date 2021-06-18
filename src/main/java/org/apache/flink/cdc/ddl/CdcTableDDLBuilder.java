package org.apache.flink.cdc.ddl;

import org.apache.flink.configuration.Configuration;

public class CdcTableDDLBuilder implements TableDDLBuilder {
    private final String db_host;
    private final String db_port;
    private final String db_username;
    private final String db_password;

    public CdcTableDDLBuilder(String host, String port, String username, String password) {
        this.db_host = host;
        this.db_port = port;
        this.db_password = password;
        this.db_username = username;
    }

    @Override
    public String getDDLTableName(String database, String table) {
        return database + "." + table + "_cdc";
    }

    @Override
    public String getDDLString(String database, String table, String columnDef, Configuration options) {
        return String.format("CREATE TABLE %s(\n" +
                        " %s) WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '%s',\n" +
                        " 'port' = '%s',\n" +
                        " 'username' = '%s',\n" +
                        " 'password' = '%s',\n" +
                        " 'database-name' = '%s',\n" +
                        " 'table-name' = '%s',\n" +
                        " 'debezium.snapshot.locking.mode' = 'none',\n" +
                        " 'debezium.database.serverTimezone' = 'UTC',\n" +
                        " 'debezium.database.characterEncoding' = 'utf8',\n" +
                        " 'debezium.database.useUnicode' = 'true',\n" +
                        " 'debezium.database.zeroDateTimeBehavior' = 'convertToNull'\n" +
                        ")",
                getDDLTableName(database, table), columnDef,
                db_host, db_port,
                db_username, db_password,
                database, table);
    }
}
