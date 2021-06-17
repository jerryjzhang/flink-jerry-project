package org.apache.flink.util;

public class MysqlJdbcDDLBuilder extends AbstractMysqlDDLBuilder {
    public MysqlJdbcDDLBuilder(String host, String port, String username, String password) {
        super(host, port, username, password);
    }

    @Override
    public String getDDLString(String database, String table,
                               AbstractDDLBuilder.DDLContext ctx) {
        String columnDef;
        try {
            columnDef = getDDLColumnDef(database, table, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true" +
                        "&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC",
                db_host, db_port, database);
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
                db_username, db_password, table, ctx.parallelism);
    }
}
