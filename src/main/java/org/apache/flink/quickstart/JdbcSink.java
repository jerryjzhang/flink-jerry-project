package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.ArrayList;
import java.util.List;

public class JdbcSink extends BaseStreamingExample {
    static List<Tuple3<Integer, String, Integer>> elements = new ArrayList<>();
    static {
        elements.add(new Tuple3<>(1, "yangguo", 30));
        elements.add(new Tuple3<>(2, "huangrong", 40));
        elements.add(new Tuple3<>(3, "guojing", 50));
    }


    public static void main(String [] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // collection input
//        DataStream<Tuple3<Integer, String, Integer>> dataStream = env.fromCollection(elements);
//        tblEnv.registerDataStream("test", dataStream, "id, name, age");

        // kafka input
        setupKafkaEnvironment();
        tblEnv.connect(new Kafka().version("0.10")
                .topic(AVRO_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(SdkLog.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                .inAppendMode()
                .registerTableSource("test");
        // actual sql query
        Table result = tblEnv.sqlQuery("SELECT id,name,age from test");

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/test")
                .setQuery("insert into jerry values (?, ?, ?)")
                .setUsername("root")
                .setPassword("")
                .setBatchSize(1)
                .setParameterTypes(Types.INT,Types.STRING,Types.INT)
                .build();
        result.writeToSink(sink);
        TableSchema schema = new TableSchema(new String[]{"id", "name", "age"},
                new TypeInformation[]{org.apache.flink.api.common.typeinfo.Types.INT,
                        org.apache.flink.api.common.typeinfo.Types.STRING, Types.INT});
        result.writeToSink(new TestAppendSink(schema));

        env.execute();
    }
}
