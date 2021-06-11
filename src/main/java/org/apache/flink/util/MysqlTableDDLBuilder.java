package org.apache.flink.util;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MysqlTableDDLBuilder {
    static Map<String, String> typeMap = new HashMap<>();
    static {
        typeMap.put("TINYINT", "TINYINT");
        typeMap.put("SMALLINT", "SMALLINT");
        typeMap.put("VARCHAR", "STRING");
        typeMap.put("FLOAT", "FLOAT");
        typeMap.put("INT", "INT");
        typeMap.put("TIMESTAMP", "TIMESTAMP");
        typeMap.put("DATETIME", "TIMESTAMP(3)");
        typeMap.put("TEXT", "STRING");
    }

    final String db_host;
    final String db_port;
    final String db_username;
    final String db_password;
    final String db_jdbcUrl;

    public MysqlTableDDLBuilder(String host, String port, String username, String password) {
        this.db_host = host;
        this.db_port = port;
        this.db_password = password;
        this.db_username = username;
        this.db_jdbcUrl = String.format("jdbc:mysql://%s:%s?useUnicode=true&characterEncoding=utf8", host, port);
    }

    public String getCdcTableDDL(String database, String table) throws Exception {
        return getCdcTableDDL(database, table, DDLContext.EMPTY);
    }

    public String getCdcTableDDL(String database, String table, DDLContext ctx) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(db_jdbcUrl, db_username, db_password);
        Statement stmt = con.createStatement();

        ResultSet res = stmt.executeQuery(String.format("select * from %s.%s where 1<0", database, table));
        ResultSetMetaData rsmd = res.getMetaData();
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i <= rsmd.getColumnCount(); i++) {
            sb.append(rsmd.getColumnName(i) + " " + typeMap.get(rsmd.getColumnTypeName(i)));
            if (i < rsmd.getColumnCount()) {
                sb.append(",");
            }
            sb.append("\n");
        }

        if (ctx.procTimeCol != null) {
            sb.append(String.format(",%s AS PROCTIME()\n", ctx.procTimeCol));
        }

        if (ctx.keyCol != null) {
            sb.append(String.format(",PRIMARY KEY (%s) NOT ENFORCED\n", ctx.keyCol));
        }

        if (ctx.rowTimeCol != null) {
            String tkeyDef = String.format(",WATERMARK FOR %s AS %s", ctx.rowTimeCol, ctx.rowTimeCol);
            sb.append(tkeyDef);
            if (ctx.watermarkInterval != null &&  ctx.watermarkInterval > 0) {
                sb.append(String.format(" + INTERVAL '%d' SECONDS", ctx.watermarkInterval));
            } else if(ctx.watermarkInterval != null &&  ctx.watermarkInterval < 0) {
                sb.append(String.format(" - INTERVAL '%d' SECONDS", 0 - ctx.watermarkInterval));
            }
        }

        String columnDef = sb.toString();
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
                    database+"."+table, columnDef,
                    db_host, db_port,
                    db_username, db_password,
                    database, table);
    }

    public String getJdbcTableDDL(String database, String table, String keyCol)throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(db_jdbcUrl, db_username, db_password);
        Statement stmt = con.createStatement();
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true" +
                        "&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC",
                db_host, db_port, database);

        ResultSet res = stmt.executeQuery(String.format("select * from %s.%s where 1<0", database, table));
        ResultSetMetaData rsmd = res.getMetaData();
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i <= rsmd.getColumnCount(); i++) {
            sb.append(rsmd.getColumnName(i) + " " + typeMap.get(rsmd.getColumnTypeName(i)));
            if (i < rsmd.getColumnCount()) {
                sb.append(",");
            }
            sb.append("\n");
        }

        if (keyCol != null) {
            String pkeyDef = String.format(",PRIMARY KEY (%s) NOT ENFORCED\n", keyCol);
            sb.append(pkeyDef);
        }
        String columnsDef = sb.toString();

        return String.format(
                "CREATE TABLE %s (\n" +
                        " %s) WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = '%s',\n" +
                        " 'username' = '%s',\n" +
                        " 'password' = '%s',\n" +
                        " 'table-name' = '%s'\n" +
                        ")",
                database+"."+table, columnsDef, jdbcUrl, db_username, db_password, table);
    }

    public static class DDLContext {
        public String keyCol;
        public String rowTimeCol;
        public Integer watermarkInterval;
        public String procTimeCol;

        public static DDLContext EMPTY = new DDLContext();

        public DDLContext keyCol(String keyCol) {
            this.keyCol = keyCol;
            return this;
        }
        public DDLContext rowTimeCol(String rowTimeCol) {
            this.rowTimeCol = rowTimeCol;
            return this;
        }
        public DDLContext watermarkInterval(Integer watermarkInterval) {
            this.watermarkInterval = watermarkInterval;
            return this;
        }
        public DDLContext procTimeCol(String procTimeCol) {
            this.procTimeCol = procTimeCol;
            return this;
        }
    }

}
