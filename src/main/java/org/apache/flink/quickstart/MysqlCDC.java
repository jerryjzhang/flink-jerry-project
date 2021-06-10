package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCDC {
    public static void main(String [] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        String sourceDDL = String.format(
                "CREATE TABLE product_source (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight DECIMAL(10,3)," +
                        " PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector' = 'mysql-cdc'," +
                        " 'hostname' = '%s'," +
                        " 'port' = '%s'," +
                        " 'username' = '%s'," +
                        " 'password' = '%s'," +
                        " 'database-name' = '%s'," +
                        " 'table-name' = '%s'," +
                        " 'debezium.database.characterEncoding' = 'utf8'," +
                        " 'debezium.database.useUnicode' = 'true'" +
                        ")",
                "localhost", 3306, "jerryjzhang", "tme", "jerry", "products");

        String sinkDDL = "CREATE TABLE product_print (" +
                " id INT," +
                " name STRING," +
                " weight DECIMAL(10,3)," +
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

        String sinkMysqlDDL = String.format(
                "CREATE TABLE product_sink_mysql (" +
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
                "jdbc:mysql://localhost:3306/jerry", "jerryjzhang", "tme", "products_sink");

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

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(sinkEsDDL);
        tEnv.executeSql(sinkMysqlDDL);
        tEnv.executeSql(sinkPostgresDDL);

        tEnv.executeSql("INSERT INTO product_es SELECT id, name, weight FROM " +
                "product_source");
        tEnv.executeSql("INSERT INTO product_print SELECT id, description, weight FROM " +
                "product_source");
        tEnv.executeSql("INSERT INTO product_sink_mysql SELECT * FROM " +
                "product_source");
        tEnv.executeSql("INSERT INTO product_sink_postgres SELECT * FROM " +
                "product_source");
    }
}
