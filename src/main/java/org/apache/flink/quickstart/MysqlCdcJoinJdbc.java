package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.MysqlTableDDLBuilder;

public class MysqlCdcJoinJdbc {
    static String source_db_host = "localhost";
    static String source_db_port = "3306";
    static String source_db_username = "jerryjzhang";
    static String source_db_password = "tme";

    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        MysqlTableDDLBuilder tableFactory = new MysqlTableDDLBuilder(source_db_host, source_db_port,
                source_db_username, source_db_password);
        String sourceDDL = tableFactory.getCdcTableDDL("jerry", "products");
        String dimDDL = tableFactory.getJdbcTableDDL("jessie", "products", "id");

        String printDDL = "create table product_print (" +
                "id INT," +
                "name STRING," +
                "description STRING" +
                ") WITH (" +
                " 'connector' = 'print'\n" +
                " )";
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jerry");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS jessie");
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(dimDDL);
        tEnv.executeSql(printDDL);

        tEnv.executeSql("INSERT INTO product_print " +
                "SELECT T.id, T.name, E.description " +
                "FROM jerry.products AS T " +
                "LEFT JOIN jessie.products FOR SYSTEM_TIME AS OF T.proctime AS E " +
                "ON T.id = E.id");
    }
}
