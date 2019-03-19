package org.apache.flink.quickstart;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class StreamingFileSinkDemo extends BaseStreamingExample {
    public static void main(String [] args) throws Exception {
        setupKafkaEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.getTableEnvironment(env);
        env.enableCheckpointing(2000);

        tableAPI(tblEnv);

        env.execute();
    }

    public static void tableAPI(StreamTableEnvironment tblEnv) {
        tblEnv.connect(new Kafka().version("0.10")
                .topic(AVRO_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(SdkLog.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                .inAppendMode()
                .registerTableSource("test");

        tblEnv.registerTableSink("hdfsTable", new String[]{"id"}, new TypeInformation[]{Types.INT}, new StreamingFileTableSink());

        tblEnv.sqlUpdate("insert into hdfsTable select id from test");
    }

    public static void directAPI(StreamExecutionEnvironment env) {
        DataStream<SdkLog> stream = env.addSource(new FlinkKafkaConsumer011<>(AVRO_INPUT_TOPIC,
                AvroDeserializationSchema.forSpecific(SdkLog.class), kafkaProps).setStartFromEarliest());

        StreamingFileSink fileSink = StreamingFileSink
                .forRowFormat(new Path("hdfs://localhost:9020/tmp"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.create().withRolloverInterval(1000L)
                        //.withInactivityInterval(1000L)
                        //.withMaxPartSize(100L)
                        .build())
                .build();
        stream.addSink(fileSink);
    }

    public static class StreamingFileTableSink implements AppendStreamTableSink<Row> {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        @Override
        public void emitDataStream(DataStream dataStream) {
            try {
                dataStream.addSink(StreamingFileSink
                        .forRowFormat(new Path("/tmp"), new SimpleStringEncoder<>("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.create().withRolloverInterval(1000L)
                                //.withInactivityInterval(1000L)
                                //.withMaxPartSize(100L)
                                .build())
                        .build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TypeInformation getOutputType() {
            return new RowTypeInfo(fieldTypes);
        }

        @Override
        public String[] getFieldNames() {
            return this.fieldNames;
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return this.fieldTypes;
        }

        @Override
        public TableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            StreamingFileTableSink tableSink = new StreamingFileTableSink();
            tableSink.fieldNames = fieldNames;
            tableSink.fieldTypes = fieldTypes;
            return tableSink;
        }
    }
}
