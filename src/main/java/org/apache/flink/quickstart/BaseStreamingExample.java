package org.apache.flink.quickstart;

import com.oppo.dc.data.avro.generated.PositionLog;
import com.oppo.dc.data.avro.generated.UserClick;
import com.oppo.dc.data.avro.generated.UserExpose;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract public class BaseStreamingExample {
    protected static final String AVRO_INPUT_TOPIC = "inputAvro";
    protected static final String POS_INPUT_TOPIC = "inputPosition";
    protected static final String CSV_INPUT_TOPIC = "inputCsv";
    protected static final String OUTPUT_TOPIC = "output";
    protected static final String KAFKA_CONN_STR = "localhost:5001";
    protected static final String Zk_CONN_STR = "localhost:5000";

    protected static final String USER_CLICK_TOPIC = "userClick";
    protected static final String USER_EXPOSE_TOPIC = "userExpose";

    private static KafkaUnit kafkaServer;

    private static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // kafka related configs
    protected static Properties kafkaProps = new Properties();
    static {
        kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
        kafkaProps.put("zookeeper.connect", Zk_CONN_STR);
        kafkaProps.put("group.id", "jerryConsumer");
        kafkaProps.put("key.serializer", ByteArraySerializer.class.getName());
        kafkaProps.put("value.serializer", ByteArraySerializer.class.getName());
    }

    protected static void setupKafkaEnvironment()throws Exception {
        kafkaServer = new KafkaUnit(Zk_CONN_STR, KAFKA_CONN_STR);
        kafkaServer.startup();
        kafkaServer.createTopic(AVRO_INPUT_TOPIC);
        kafkaServer.createTopic(CSV_INPUT_TOPIC);
        kafkaServer.createTopic(POS_INPUT_TOPIC);
        kafkaServer.createTopic(OUTPUT_TOPIC);
        kafkaServer.createTopic(USER_EXPOSE_TOPIC);
        kafkaServer.createTopic(USER_CLICK_TOPIC);

        KafkaProducer producer = new KafkaProducer(kafkaProps);
        generateCSVMessages(producer);
        generateAvroMessages(producer);
        generatePosMessages(producer);

        ProduceMessageThread thread = new ProduceMessageThread(producer);
        scheduler.scheduleWithFixedDelay(thread, 0, 5, TimeUnit.SECONDS);
    }

    protected static void generateCSVMessages(KafkaProducer producer)throws Exception {
        ProducerRecord<String,byte[]> message = new ProducerRecord<>(CSV_INPUT_TOPIC, null,
                "1\tyangguo\t30\teventTag:10004,eventId:1000".getBytes());
        producer.send(message);

        ProducerRecord<String,byte[]> message1 = new ProducerRecord<>(CSV_INPUT_TOPIC, null,
                "2\tguojing\t50\teventTag:10003,eventId:1001".getBytes());
        producer.send(message1);

        ProducerRecord<String,byte[]> message2 = new ProducerRecord<>(CSV_INPUT_TOPIC, null,
                "3\thuangrong\t40\teventTag:10003,eventId:1001".getBytes());
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

        sendAvroMessagesInternal(producer, AVRO_INPUT_TOPIC, record, SdkLog.getClassSchema());

        record = SdkLog.newBuilder()
                .setId(2)
                .setName("jinyong")
                .setAge(94)
                .setEvent(event)
                .build();
        sendAvroMessagesInternal(producer, AVRO_INPUT_TOPIC, record, SdkLog.getClassSchema());
    }

    protected static void generatePosMessages(KafkaProducer producer)throws Exception {
        PositionLog log = PositionLog.newBuilder()
                .setImei("imei")
                .setSsoid("ssoid")
                .setLat("40.11")
                .setLon("-70")
                .setServerTime(System.currentTimeMillis())
                .setClientTime(System.currentTimeMillis())
                .build();

        sendAvroMessagesInternal(producer, POS_INPUT_TOPIC, log, PositionLog.getClassSchema());
    }

    protected static void generateCTRMessages(KafkaProducer producer)throws Exception {
        UserExpose expose = UserExpose.newBuilder()
                .setImei("imei1")
                .setSource("source1")
                .setTime(System.currentTimeMillis())
                .build();
        sendAvroMessagesInternal(producer, USER_EXPOSE_TOPIC, expose, UserExpose.getClassSchema());

        expose = UserExpose.newBuilder()
                .setImei("imei1")
                .setSource("source1")
                .setTime(System.currentTimeMillis())
                .build();
        sendAvroMessagesInternal(producer, USER_EXPOSE_TOPIC, expose, UserExpose.getClassSchema());

        UserClick click = UserClick.newBuilder()
                .setImei("imei1")
                .setSource("source1")
                .setTime(System.currentTimeMillis())
                .build();
        sendAvroMessagesInternal(producer, USER_CLICK_TOPIC, click, UserClick.getClassSchema());
    }

    private static void sendAvroMessagesInternal(KafkaProducer producer, String topic,
                                             Object data, org.apache.avro.Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter writer = new SpecificDatumWriter<>(schema);
        writer.write(data, encoder);
        encoder.flush();
        out.close();

        ProducerRecord<String,byte[]> message = new ProducerRecord<>(topic, null,
                out.toByteArray());
        producer.send(message);
    }

    static class ProduceMessageThread implements Runnable {
        private KafkaProducer producer;

        public ProduceMessageThread(KafkaProducer producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            try {
                generateCTRMessages(producer);
            } catch (Exception e) {

            }
        }
    }
}
