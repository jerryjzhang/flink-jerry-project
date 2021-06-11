package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.RandomFibonacciSource;

import static org.apache.flink.table.api.Expressions.$;

public class MysqlStreamJoinJdbc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        String dimDDL = String.format(
                "CREATE TABLE dimTable (" +
                        " id INT NOT NULL," +
                        " name STRING," +
                        " description STRING," +
                        " weight FLOAT," +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (" +
                        " 'connector' = 'jdbc'," +
                        " 'url' = '%s'," +
                        " 'username' = '%s'," +
                        " 'password' = '%s'," +
                        " 'table-name' = '%s'" +
                        ")",
                "jdbc:mysql://localhost:3306/jerry", "jerryjzhang", "tme", "products");

        String sinkDDL = "CREATE TABLE sinkTable (" +
                " a INT," +
                " b INT," +
                " t TIMESTAMP(9)," +
                " name STRING," +
                " PRIMARY KEY (a) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tEnv.createTemporaryView("sourceTable", env.addSource(new RandomFibonacciSource()),
                $("a"), $("b"), $("t"), $("pt").proctime());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(dimDDL);

        tEnv.executeSql("insert into sinkTable " +
                "select T.a, T.b, T.pt, D.name " +
                "from sourceTable AS T " +
                "LEFT JOIN dimTable FOR SYSTEM_TIME AS OF T.pt AS D " +
                "ON T.a = D.id");
        //tEnv.executeSql("insert into sinkTable select T.*, 'jerry' from sourceTable as T");
    }
}
