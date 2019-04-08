package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
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
        StreamTableEnvironment tblEnv = StreamTableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple3<Integer, String, Integer>> dataStream = env.fromCollection(elements);

        // collection input
        tblEnv.registerDataStream("test", dataStream, "id, name, age, tt.proctime");

        tblEnv.registerFunction("lookup", new LookUp());
        tblEnv.registerFunction("feature", new FeatureExtract());

        // cross join UDF
        Table collectionTable = tblEnv.sqlQuery("SELECT lookup(fname), fage, feature FROM (SELECT lookup(id, name) as fname, fage, feature from test, " +
                "LATERAL TABLE(feature(age)) as T(fage, feature))");
        // left join UDF
        //Table collectionTable = tblEnv.sqlQuery("SELECT lookup(fname), fage, feature FROM (SELECT lookup(id, name) as fname, fage, feature from test LEFT JOIN " +
        //        "LATERAL TABLE(feature(age)) as T(fage, feature) ON TRUE)");
        collectionTable.writeToSink(new TestAppendSink(
                new TableSchema(new String[]{"name", "age", "feature"}, new TypeInformation[]{Types.STRING, Types.INT, Types.STRING})));

        env.execute();
    }

    public static class LookUp extends ScalarFunction {
        public String eval(int id, String name) {
            return id + "-" + name;
        }

        public String eval(String name) {
            return "s" + name;
        }
    }

    public static class FeatureExtract extends TableFunction<Tuple2<Integer, String>> {
        public void eval(int age) {
            if(age >= 50) {
                collect(new Tuple2<>(age, "feature1"));
                collect(new Tuple2<>(age, "feature2"));
            }
        }

        @Override
        public TypeInformation<Tuple2<Integer, String>> getResultType() {
            return Types.TUPLE(Types.INT, Types.STRING);
        }
    }
}
