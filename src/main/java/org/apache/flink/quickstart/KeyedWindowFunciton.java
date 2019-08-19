package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeyedWindowFunciton extends BaseStreamingExample {
    static List<Tuple3<Integer, String, Integer>> elements = new ArrayList<>();
    static {
        elements.add(new Tuple3<>(1, "yangguo", 30));
        elements.add(new Tuple3<>(2, "huangrong", 40));
        elements.add(new Tuple3<>(3, "guojing", 50));
    }

    public static void main(String [] args)throws Exception {
        setupKafkaEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<SdkLog> stream = env.addSource(new FlinkKafkaConsumer011<>(AVRO_INPUT_TOPIC,
                AvroDeserializationSchema.forSpecific(SdkLog.class), kafkaProps).setStartFromEarliest());

        DataStream sdk_feeds = stream.filter(new FilterFunction<SdkLog>(){
            @Override
            public boolean filter(SdkLog value) throws Exception {
                return false;
            }
        });

        DataStream sdk_cdo = stream.filter(new FilterFunction<SdkLog>(){
            @Override
            public boolean filter(SdkLog value) throws Exception {
                return false;
            }
        });

        sdk_feeds
                .addSink(new FlinkKafkaProducer011(OUTPUT_TOPIC, new MyAvroSerializationSchema(), kafkaProps));
        sdk_cdo
                .addSink(new FlinkKafkaProducer011(OUTPUT_TOPIC, new MyAvroSerializationSchema(), kafkaProps));


        System.out.println(env.getExecutionPlan());
    }

    static class MyAvroSerializationSchema implements KeyedSerializationSchema<SdkLog> {
        private AvroSerializer<SdkLog> serializer = new AvroSerializer<>(SdkLog.class);

        @Override
        public byte[] serializeKey(SdkLog element) {
            return element.getName().toString().getBytes();
        }

        @Override
        public byte[] serializeValue(SdkLog element) {
            DataOutputSerializer dos = new DataOutputSerializer(1024);
            try {
                serializer.serialize(element, dos);
            } catch (IOException e) {
                // ignore so far
            }

            return dos.getCopyOfBuffer();
        }

        @Override
        public String getTargetTopic(SdkLog element) {
            return null;
        }
    }
}
