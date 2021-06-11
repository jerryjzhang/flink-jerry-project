package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlTableDDLBuilder;

public class MysqlCDC {
    public static void main(String [] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        MysqlTableDDLBuilder builder = new MysqlTableDDLBuilder("localhost", "3306",
                "jerryjzhang", "tme");
        String sourceDDL = builder.getCdcTableDDL("jerry", "products");
        String sinkMysqlDDL = builder.getJdbcTableDDL("jerry", "products_sink", "id");

        String sinkDDL = "CREATE TABLE product_print (" +
                " id INT," +
                " name STRING," +
                " weight DECIMAL(10,3)," +
                " update_time TIMESTAMP," +
                " PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        String sinkEsDDL = "CREATE TABLE product_es (\n" +
                " id INT," +
                "name STRING,\n" +
                "weight DECIMAL(10,3),\n" +
                "PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'jerry_product'\n" +
                ")";

        String sinkPostgresDDL = String.format(
                "CREATE TABLE product_sink_postgres (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (" +
                        " 'connector' = 'jdbc'," +
                        " 'url' = '%s'," +
                        " 'username' = '%s'," +
                        " 'password' = '%s'," +
                        " 'table-name' = '%s'" +
                        ")",
                "jdbc:postgresql://localhost:5432/postgres", "postgres", "tme", "products");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jerry");
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(sinkEsDDL);
        tEnv.executeSql(sinkMysqlDDL);
        tEnv.executeSql(sinkPostgresDDL);

//        tEnv.executeSql("INSERT INTO product_es SELECT id, name, weight FROM " +
//                "jerry.products");
        tEnv.executeSql("INSERT INTO product_print " +
                "SELECT id, description, weight, update_time " +
                "FROM jerry.products");
        tEnv.executeSql("INSERT INTO jerry.products_sink " +
                "SELECT id,name,description,weight,update_time " +
                "FROM jerry.products");
    }
}
