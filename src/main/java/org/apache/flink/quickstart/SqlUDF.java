package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.ArrayList;
import java.util.List;

public class SqlUDF {
    static List<Tuple3<Integer, String, Integer>> elements = new ArrayList<>();
    static {
        elements.add(new Tuple3<>(1, "yangguo", 30));
        elements.add(new Tuple3<>(2, "huangrong", 40));
        elements.add(new Tuple3<>(3, "guojing", 50));
    }

    public static void main(String [] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple3<Integer, String, Integer>> dataStream = env.fromCollection(elements);

        // collection input
        tEnv.createTemporaryView("test", dataStream, "id, name, age, tt.proctime");

        tEnv.createTemporarySystemFunction("lookup", new LookUp());
        tEnv.createTemporarySystemFunction("feature", new FeatureExtract());

        // cross join UDF
        Table collectionTable = tEnv.sqlQuery("SELECT lookup(fname), fage, feature FROM (SELECT lookup(id, name) as fname, fage, feature from test, " +
                "LATERAL TABLE(feature(age)) as T(fage, feature))");
        // left join UDF
        //Table collectionTable = tblEnv.sqlQuery("SELECT lookup(fname), fage, feature FROM (SELECT lookup(id, name) as fname, fage, feature from test LEFT JOIN " +
        //        "LATERAL TABLE(feature(age)) as T(fage, feature) ON TRUE)");
        tEnv.toAppendStream(collectionTable,
                Types.ROW(Types.STRING, Types.INT, Types.STRING)).printToErr();

        env.execute();
    }

    public static class LookUp extends ScalarFunction {
        public String eval(Integer id, String name) {
            return id + "-" + name;
        }

        public String eval(String name) {
            return "s" + name;
        }
    }

    public static class FeatureExtract extends TableFunction<Tuple2<Integer, String>> {
        public void eval(Integer age) {
            if(age >= 50) {
                collect(new Tuple2<>(age, "feature1"));
                collect(new Tuple2<>(age, "feature2"));
            }
        }
    }
}
