package org.apache.flink.util;

public class MysqlCdcDDLBuilder extends AbstractMysqlDDLBuilder {
    public MysqlCdcDDLBuilder(String host, String port, String username, String password) {
        super(host, port, username, password);
    }

    @Override
    public String getDDLString(String database, String table,
                               DDLContext ctx) {
        String columnDef;
        try {
            columnDef = getDDLColumnDef(database, table, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

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
