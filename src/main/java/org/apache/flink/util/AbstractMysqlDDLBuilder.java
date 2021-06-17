package org.apache.flink.util;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMysqlDDLBuilder extends AbstractDDLBuilder {
    private static Map<String, String> typeMap = new HashMap<>();
    static {
        typeMap.put("TINYINT", "TINYINT");
        typeMap.put("SMALLINT", "SMALLINT");
        typeMap.put("BIGINT", "BIGINT");
        typeMap.put("VARCHAR", "STRING");
        typeMap.put("FLOAT", "FLOAT");
        typeMap.put("INT", "BIGINT");
        typeMap.put("INT UNSIGNED", "BIGINT");
        typeMap.put("TIMESTAMP", "TIMESTAMP");
        typeMap.put("DATETIME", "TIMESTAMP(3)");
        typeMap.put("TEXT", "STRING");
        typeMap.put("LONGTEXT", "STRING");
    }

    protected final String db_host;
    protected final String db_port;
    protected final String db_username;
    protected final String db_password;
    protected final String db_jdbcUrl;

    public AbstractMysqlDDLBuilder(String host, String port, String username, String password) {
        this.db_host = host;
        this.db_port = port;
        this.db_password = password;
        this.db_username = username;
        this.db_jdbcUrl = String.format("jdbc:mysql://%s:%s?useUnicode=true&characterEncoding=utf8", host, port);
    }

    @Override
    public String getDDLTableName(String database, String table) {
        return database + "." + table + "_db";
    }

    public String getDDLColumnDef(String database, String table,
                                  DDLContext ctx) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con = DriverManager.getConnection(db_jdbcUrl, db_username, db_password);
        Statement stmt = con.createStatement();

        ResultSet res = stmt.executeQuery(String.format("select * from %s.%s where 1<0", database, table));
        ResultSetMetaData rsmd = res.getMetaData();
        StringBuilder sb = new StringBuilder();
        int columnCnt = 1;
        for(int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (ctx.includeColumns != null && !ctx.includeColumns.contains(rsmd.getColumnName(i))) {
                continue;
            }
            if (columnCnt++ > 1) {
                sb.append(",");
            }
            sb.append(rsmd.getColumnName(i) + " " + typeMap.get(rsmd.getColumnTypeName(i)));
            sb.append("\n");
        }

        if (ctx.procTimeCol != null) {
            sb.append(String.format(",%s AS PROCTIME()\n", ctx.procTimeCol));
        }

        DatabaseMetaData dm = con.getMetaData();
        ResultSet rs = dm.getPrimaryKeys( null , null , table);
        if (rs.next()) {
            String pkey = rs.getString("COLUMN_NAME");
            sb.append(String.format(",PRIMARY KEY (%s) NOT ENFORCED\n", pkey));
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

        return sb.toString();
    }

    @Override
    public abstract String getDDLString(String database, String table, DDLContext ctx);
}
