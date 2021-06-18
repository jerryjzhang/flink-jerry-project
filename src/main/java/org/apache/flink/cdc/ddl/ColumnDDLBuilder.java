package org.apache.flink.cdc.ddl;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnDDLBuilder {
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
        typeMap.put("DOUBLE", "DOUBLE");
    }

    protected final String db_host;
    protected final String db_port;
    protected final String db_username;
    protected final String db_password;
    protected final String db_jdbcUrl;

    public ColumnDDLBuilder(String host, String port, String username, String password) {
        this.db_host = host;
        this.db_port = port;
        this.db_password = password;
        this.db_username = username;
        this.db_jdbcUrl = String.format("jdbc:mysql://%s:%s?useUnicode=true&characterEncoding=utf8", host, port);
    }
    
    public String getDDLString(String database, String table, DDLContext ctx) throws Exception {
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

        String pkey = null;
        if (ctx.keyCol != null) {
            pkey = ctx.keyCol;
        } else {
            DatabaseMetaData dm = con.getMetaData();
            ResultSet rs = dm.getPrimaryKeys(null, null, table);
            if (rs.next()) {
                pkey = rs.getString("COLUMN_NAME");
            }
        }
        sb.append(String.format(",PRIMARY KEY (%s) NOT ENFORCED\n", pkey));

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


    public static class DDLContext {
        public String keyCol;
        public String rowTimeCol;
        public String procTimeCol;
        public Integer watermarkInterval;
        public List<String> includeColumns;

        public static DDLContext EMPTY = new DDLContext();

        public  DDLContext keyCol(String keyCol) {
            this.keyCol = keyCol;
            return this;
        }
        public  DDLContext rowTimeCol(String rowTimeCol) {
            this.rowTimeCol = rowTimeCol;
            return this;
        }
        public  DDLContext watermarkInterval(Integer watermarkInterval) {
            this.watermarkInterval = watermarkInterval;
            return this;
        }
        public  DDLContext procTimeCol(String procTimeCol) {
            this.procTimeCol = procTimeCol;
            return this;
        }
        public  DDLContext includedColumns(List<String> includeColumns) {
            this.includeColumns = includeColumns;
            return this;
        }
    }
}
