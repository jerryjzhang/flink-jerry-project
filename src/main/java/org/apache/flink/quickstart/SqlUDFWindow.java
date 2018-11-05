package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SqlUDFWindow extends BaseStreamingExample {
    static List<Tuple3<Integer, String, Integer>> elements = new ArrayList<>();
    static {
        elements.add(new Tuple3<>(1, "yangguo", 30));
        elements.add(new Tuple3<>(2, "huangrong", 40));
        elements.add(new Tuple3<>(3, "guojing", 50));
    }

    public static void main(String [] args)throws Exception {
        // setup local kafka environment
        setupKafkaEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple3<Integer, String, Integer>> dataStream = env.fromCollection(elements);

        // collection input
        tblEnv.registerDataStream("test", dataStream, "id, name, age, tt.proctime");
        // kafka input
        tblEnv.connect(new Kafka().version("0.10")
                .topic(CSV_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Csv().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class)))
                        .field("tt", Types.SQL_TIMESTAMP).proctime())
                .inAppendMode()
                .registerTableSource("test2");

        tblEnv.registerFunction("wAvg", new WeightedAvg());
        tblEnv.registerFunction("wDouble", new DoubleOutput());


        Table kafkaTable = tblEnv.sqlQuery("SELECT sTime, tAge FROM (SELECT TUMBLE_END(tt, INTERVAL '1' SECOND) as sTime, wAvg(age) as wAge " +
                "FROM test2 GROUP BY TUMBLE(tt, INTERVAL '1' SECOND)), LATERAL TABLE(wDouble(wAge)) as T(tAge)");
        kafkaTable.writeToSink(new TestAppendSink(
                new TableSchema(new String[]{"time", "age"}, new TypeInformation[]{Types.SQL_TIMESTAMP, Types.INT})));

//        Table collectionTable = tblEnv.sqlQuery("SELECT TUMBLE_END(tt, INTERVAL '1' SECOND), MAX(age) from test GROUP BY TUMBLE(tt, INTERVAL '1' SECOND)");
//        collectionTable.writeToSink(new TestAppendSink(
//                new TableSchema(new String[]{"time", "age"}, new TypeInformation[]{Types.SQL_TIMESTAMP, Types.INT})));

        env.execute();
    }

    public static class AvgAccum {
        public int sum = 0;
        public int count = 0;
    }

    public static class WeightedAvg extends AggregateFunction<Integer, AvgAccum> {

        @Override
        public AvgAccum createAccumulator() {
            return new AvgAccum();
        }

        @Override
        public Integer getValue(AvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }

        public void accumulate(AvgAccum acc, int value) {
            acc.sum += value * 1;
            acc.count += 1;
        }

        public void retract(AvgAccum acc, int value) {
            acc.sum -= value * 1;
            acc.count -= 1;
        }

        public void merge(AvgAccum acc, Iterable<AvgAccum> it) {
            Iterator<AvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                AvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(AvgAccum acc) {
            acc.count = 0;
            acc.sum = 0;
        }
    }

    public static class DoubleOutput extends TableFunction<Integer> {
        public void eval(int age) {
            collect(age);
            collect(age * 2);
        }

        @Override
        public TypeInformation<Integer> getResultType() {
            return Types.INT;
        }
    }
}
