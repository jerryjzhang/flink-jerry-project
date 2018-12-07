package org.apache.flink.quickstart;

import com.oppo.dc.data.avro.generated.UserClick;
import com.oppo.dc.data.avro.generated.UserExpose;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

public class ComputeCTR extends BaseStreamingExample {
    public static void main(String [] args) throws Exception {
        setupKafkaEnvironment();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // kafka input
        tblEnv.connect(new Kafka().version("0.10")
                .topic(USER_EXPOSE_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(UserExpose.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(
                        AvroSchemaConverter.convertToTypeInfo(UserExpose.class)))
                        .field("tt", Types.SQL_TIMESTAMP)
                        .rowtime(new Rowtime().timestampsFromField("time").watermarksPeriodicAscending()))
                .inAppendMode()
                .registerTableSource("expose");

        tblEnv.connect(new Kafka().version("0.10")
                .topic(USER_CLICK_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(UserClick.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(
                        AvroSchemaConverter.convertToTypeInfo(UserClick.class)))
                        .field("tt", Types.SQL_TIMESTAMP)
                            .rowtime(new Rowtime().timestampsFromField("time").watermarksPeriodicAscending()))
                .inAppendMode()
                .registerTableSource("click");

        // actual sql query
        Table result = tblEnv.sqlQuery("SELECT exposeCount.source, exposeCount.sTime, clickCount.sCount / exposeCount.sCount\n" +
                "FROM\n" +
                "(SELECT source, TUMBLE_END(tt, INTERVAL '1' SECOND) as sTime, CAST(count(1) AS DOUBLE) as sCount FROM expose GROUP BY source, TUMBLE(tt, INTERVAL '1' SECOND)) AS exposeCount \n" +
                "INNER JOIN\n" +
                "(SELECT source, TUMBLE_END(tt, INTERVAL '1' SECOND) as sTime, CAST(count(1) AS DOUBLE) as sCount FROM click  GROUP BY source, TUMBLE(tt, INTERVAL '1' SECOND)) AS clickCount\n" +
                "ON\n" +
                "exposeCount.source = clickCount.source AND exposeCount.sTime = clickCount.sTime");
        TableSchema schema = new TableSchema(new String[]{"source", "time", "count"},
                new TypeInformation[]{Types.STRING, Types.SQL_TIMESTAMP, Types.DOUBLE});
        result.writeToSink(new TestAppendSink(schema));
        // execute program
        System.out.println(env.getExecutionPlan());

//        // test count(distinct imei)
//        Table result = tblEnv.sqlQuery("SELECT source, TUMBLE_END(tt, INTERVAL '1' SECOND), count(distinct imei) FROM expose GROUP BY source, TUMBLE(tt, INTERVAL '1' SECOND)");
//        TableSchema schema = new TableSchema(new String[]{"source", "time", "count"},
//                new TypeInformation[]{Types.STRING, Types.SQL_TIMESTAMP, Types.LONG});
//        result.writeToSink(new TestAppendSink(schema));

        env.execute("Flink Streaming Java API Skeleton");
    }
}
