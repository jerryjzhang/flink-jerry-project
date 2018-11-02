package org.apache.flink.quickstart;

import info.batey.kafka.unit.KafkaUnit;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.formats.avro.generated.OSInstallRecord;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.generated.SdkLogRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.io.ByteArrayOutputStream;
import java.util.*;

abstract public class BaseStreamingExample {
    protected static final String AVRO_INPUT_TOPIC = "iputAvro";
    protected static final String CSV_INPUT_TOPIC = "inputCsv";
    protected static final String OUTPUT_TOPIC = "output";
    protected static final String KAFKA_CONN_STR = "localhost:5001";
    protected static final String Zk_CONN_STR = "localhost:5000";

    private static KafkaUnit kafkaServer = new KafkaUnit(Zk_CONN_STR, KAFKA_CONN_STR);

    // kafka related configs
    protected static Properties kafkaProps = new Properties();
    static {
        kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
        kafkaProps.put("zookeeper.connect", Zk_CONN_STR);
        kafkaProps.put("group.id", "jerryConsumer");
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", BytesSerializer.class.getName());
    }

    protected static void setupKafkaEnvironment()throws Exception {
        kafkaServer.startup();
        kafkaServer.createTopic(AVRO_INPUT_TOPIC);
        kafkaServer.createTopic(CSV_INPUT_TOPIC);
        kafkaServer.createTopic(OUTPUT_TOPIC);

        KafkaProducer producer = new KafkaProducer(kafkaProps);
        generateCSVMessages(producer);
        generateAvroMessages(producer);
    }

    protected static void generateCSVMessages(KafkaProducer producer)throws Exception {
        ProducerRecord<String,Bytes> message = new ProducerRecord<>(CSV_INPUT_TOPIC, null,
                Bytes.wrap("1\tyangguo\t30\teventTag:10004,eventId:1000".getBytes()));
        producer.send(message);

        ProducerRecord<String,Bytes> message1 = new ProducerRecord<>(CSV_INPUT_TOPIC, null,
                Bytes.wrap("2\tguojing\t50\teventTag:10003,eventId:1001".getBytes()));
        producer.send(message1);

        ProducerRecord<String,Bytes> message2 = new ProducerRecord<>(CSV_INPUT_TOPIC, null,
                Bytes.wrap("3\thuangrong\t40\teventTag:10003,eventId:1001".getBytes()));
        producer.send(message2);
    }

    protected static void generateAvroMessages(KafkaProducer producer)throws Exception {
        Map<CharSequence, CharSequence> event = new HashMap<>();
        event.put("eventTag", "10004");
        event.put("eventLog", "info");
        Map<CharSequence, Integer> intMap = new HashMap<>();
        intMap.put("id", 1986);
        List<CharSequence> strs = new ArrayList<>();
        strs.add("jerryjzhang");
        Map<CharSequence, SdkLogRecord> recMap = new HashMap<>();
        SdkLogRecord r = new SdkLogRecord();
        r.setId(1986);
        recMap.put("jerry", r);
        List<OSInstallRecord> recArray = new ArrayList<>();
        OSInstallRecord or = new OSInstallRecord();
        or.setName("huni");
        recArray.add(or);
        SdkLog record = SdkLog.newBuilder()
                .setId(1)
                .setName("jerryjzhang")
                .setAge(32)
                .setEvent(event)
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<SdkLog> writer = new SpecificDatumWriter<>(SdkLog.getClassSchema());
        writer.write(record, encoder);
        encoder.flush();
        out.close();

        ProducerRecord<String,Bytes> message = new ProducerRecord<>(AVRO_INPUT_TOPIC, null,
                Bytes.wrap(out.toByteArray()));
        producer.send(message);
    }



}
