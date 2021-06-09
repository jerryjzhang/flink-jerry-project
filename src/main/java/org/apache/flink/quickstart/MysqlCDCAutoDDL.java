package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MysqlCDCAutoDDL {
    static Map<String, String> typeMap = new HashMap<>();
    static {
        typeMap.put("TINYINT", "TINYINT");
        typeMap.put("SMALLINT", "SMALLINT");
        typeMap.put("VARCHAR", "STRING");
        typeMap.put("FLOAT", "FLOAT");
        typeMap.put("INT", "INT");
        typeMap.put("TIMESTAMP", "TIMESTAMP");
    }

    static String source_db_host = "localhost";
    static String source_db_port = "3306";
    static String source_db_username = "jerryjzhang";
    static String source_db_password = "tme";
    static String source_jdbc_url = String.format("jdbc:mysql://%s:%s", source_db_host, source_db_port);

    static String getMysqlCdcDDL(String host, String port, String user,
                                 String password, String database, String table, String columnDef) {
        return String.format("CREATE TABLE %s(\n" +
                "  %s) WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '%s',\n" +
                " 'port' = '%s',\n" +
                " 'username' = '%s',\n" +
                " 'password' = '%s',\n" +
                " 'database-name' = '%s',\n" +
                " 'table-name' = '%s',\n" +
                " 'debezium.database.serverTimezone' = 'UTC'\n" +
                ")", table, columnDef, host, port, user, password, database, table);
    }

    static String getPrintDDL(String table, String columnDef) {
        return String.format("CREATE TABLE %s(\n" +
                " %s) WITH (\n" +
                " 'connector' = 'print'\n" +
                ")", table, columnDef);
    }

    static String getJdbcDDL(String host, String port, String user,
                             String password, String database, String table, String columnDef) {
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", host, port, database);
        return String.format(
            "CREATE TABLE %s (\n" +
                    " %s) WITH (\n" +
                    " 'connector' = 'jdbc',\n" +
                    " 'url' = '%s',\n" +
                    " 'username' = '%s',\n" +
                    " 'password' = '%s',\n" +
                    " 'table-name' = '%s'\n" +
                    ")",
                table + "_sink", columnDef, jdbcUrl, user, password, table);
    }

    static void registerSourceTable(StreamTableEnvironment tEnv, String database, String table) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(
                source_jdbc_url, source_db_username,source_db_password);
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

        DatabaseMetaData dm = con.getMetaData( );
        ResultSet rs = dm.getExportedKeys( null , database , table);
        if (rs.next()) {
            String pkey = rs.getString("PKCOLUMN_NAME");
            String pkeyDef = String.format(",PRIMARY KEY (%s) NOT ENFORCED", pkey);
            sb.append(pkeyDef);
        }

        String columnDef = sb.toString();
        String sourceDDL = getMysqlCdcDDL(source_db_host,source_db_port,source_db_username, source_db_password, database,table, columnDef);
        String sinkDDL = getJdbcDDL(source_db_host,source_db_port,source_db_username, source_db_password, database,table, columnDef);
        System.out.println(sourceDDL);

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
    }

    public static void main(String [] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/jerry","jerryjzhang","tme");
        Statement stmt = con.createStatement();

        ResultSet res = stmt.executeQuery("select * from products where 1<0");
        ResultSetMetaData rsmd = res.getMetaData();
        rsmd.getColumnType(1);
        rsmd.getColumnLabel(1);
        rsmd.getColumnDisplaySize(1);
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i <= rsmd.getColumnCount(); i++) {
            sb.append(rsmd.getColumnName(i) + " " + typeMap.get(rsmd.getColumnTypeName(i)));
            if (i < rsmd.getColumnCount()) {
                sb.append(",");
            }
            sb.append("\n");
        }

        DatabaseMetaData dm = con.getMetaData( );
        ResultSet rs = dm.getExportedKeys( null , null , "products" );
        if (rs.next()) {
            String pkey = rs.getString("PKCOLUMN_NAME");
            String pkeyDef = String.format(",PRIMARY KEY (%s) NOT ENFORCED", pkey);
            sb.append(pkeyDef);
        }

        String columnDef = sb.toString();
        String sourceDDL = getMysqlCdcDDL("localhost","3306","jerryjzhang", "tme", "jerry","products", columnDef);
        String sinkDDL = getPrintDDL("products_print", columnDef);
        String jdbcDDL = getJdbcDDL("localhost","3306","jerryjzhang", "tme", "jerry","products_sink", columnDef);
        System.out.println(jdbcDDL);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(jdbcDDL);
        tEnv.executeSql("INSERT INTO products_print SELECT * FROM products");
        tEnv.executeSql("INSERT INTO products_sink_sink SELECT * FROM products");
    }
}
