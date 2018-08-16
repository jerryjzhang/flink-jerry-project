/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.quickstart;

import info.batey.kafka.unit.KafkaUnit;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.formats.avro.generated.OSInstallRecord;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.generated.SdkLogRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka011AvroTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;


import java.io.ByteArrayOutputStream;
import java.util.*;

public class StreamingJobAvro {
	private static final String INPUT_TOPIC = "testJerry";
	private static final String OUTPUT_TOPIC = "outputJerry";
	private static final String KAFKA_CONN_STR = "localhost:5001";
	private static final String Zk_CONN_STR = "localhost:5000";
	private static KafkaUnit kafkaServer = new KafkaUnit(Zk_CONN_STR, KAFKA_CONN_STR);

	public static void main(String[] args) throws Exception {
		// setup local kafka environment
		setupKafkaEnvironment();

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
		kafkaProps.put("zookeeper.connect", Zk_CONN_STR);
		kafkaProps.put("group.id", "jerryConsumer");
		// kafka input
        final Map<String, String> tableAvroMapping = new HashMap<>();
        tableAvroMapping.put("id", "id");
        tableAvroMapping.put("name", "name");
        tableAvroMapping.put("age", "age");
        tableAvroMapping.put("event", "event");
        tableAvroMapping.put("intMap", "intMap");
        tableAvroMapping.put("strArray", "strArray");
		tableAvroMapping.put("recMap", "recMap");
		tableAvroMapping.put("recArray", "recArray");
		Kafka011AvroTableSource.Builder builder = Kafka011AvroTableSource.builder();
		Kafka011AvroTableSource kafkaTable = builder.forTopic(INPUT_TOPIC)
				.forAvroRecordClass(SdkLog.class)
				.withSchema(TableSchema.builder()
						.field("id", Types.INT)
						.field("name", Types.STRING)
						.field("age", Types.INT)
						.field("event", Types.MAP(Types.STRING, Types.STRING))
						.field("intMap", Types.MAP(Types.STRING, Types.INT))
						.field("strArray", Types.OBJECT_ARRAY(Types.STRING))
						.field("recMap", Types.MAP(Types.STRING,
								AvroSchemaConverter.convertToTypeInfo(SdkLogRecord.class)))
						.field("recArray", Types.OBJECT_ARRAY(
								AvroSchemaConverter.convertToTypeInfo(OSInstallRecord.class)))
						.build())
				.withTableToAvroMapping(tableAvroMapping)
				.fromEarliest()
				.withKafkaProperties(kafkaProps)
				.build();
		tblEnv.registerTableSource("test", kafkaTable);

		tblEnv.registerFunction("doubleFunc", new DoubleInt());

		// actual sql query
		Table result = tblEnv.sqlQuery("SELECT id,name,doubleFunc(age) from test where event['eventTag'] = '10004' " +
				"and recMap['jerry'].id = 1986 and strArray[1] = 'jerryjzhang' and recArray[1].name = 'huni'");
		// kafka output
		TableSchema outputSchema = TableSchema.builder()
				.field("id", Types.INT)
				.field("name", Types.STRING)
				.field("age", Types.INT)
				.build();
		Kafka011TableSink sink = new Kafka011TableSink(outputSchema, OUTPUT_TOPIC,
				kafkaProps, Optional.of(new FlinkFixedPartitioner<>()), new JsonRowSerializationSchema(outputSchema.toRowType()));
		result.writeToSink(new CsvTableSink("/tmp/jerryjzhang", ","));
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	private static void setupKafkaEnvironment()throws Exception{
		kafkaServer.startup();
		kafkaServer.createTopic(INPUT_TOPIC);
		kafkaServer.createTopic(OUTPUT_TOPIC);

		Properties props = new Properties();
		props.put("bootstrap.servers", KAFKA_CONN_STR);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", BytesSerializer.class.getName());
		KafkaProducer producer = new KafkaProducer(props);

		ProducerRecord<String,Bytes> message = new ProducerRecord<>(INPUT_TOPIC, null,
				Bytes.wrap(generateTestMessage()));
		producer.send(message);
	}

	private static byte[] generateTestMessage()throws Exception{
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
				.setIntMap(intMap)
				.setStrArray(strs)
				.setRecMap(recMap)
				.setRecArray(recArray)
				.build();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<SdkLog> writer = new SpecificDatumWriter<>(SdkLog.getClassSchema());
		writer.write(record, encoder);
		encoder.flush();
		out.close();
		return out.toByteArray();
	}

	public static class DoubleInt extends ScalarFunction {
		public int eval(int s) {
			return s * 2;
		}
	}
}
