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
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.avro.generated.OSInstallRecord;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.generated.SdkLogRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalogTableBuilder;
import org.apache.flink.table.catalog.ExternalTableUtil;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class StreamingJobAvroCatalog {
	private static final String AVRO_SCHEMA = "{\n" +
			"         \"type\": \"record\",\n" +
			"         \"name\": \"SdkLog\",\n" +
			"         \"fields\": [\n" +
			"             {\"name\": \"id\", \"type\": \"int\"},\n" +
			"             {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n" +
			"             {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n" +
			"             {\"name\":\"event\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}]" +
			"}";
	private static final String OUTPUT_AVRO_SCHEMA = "{\n" +
			"         \"type\": \"record\",\n" +
			"         \"name\": \"SdkLogOutput\",\n" +
			"         \"fields\": [\n" +
			"             {\"name\": \"id\", \"type\": \"int\"},\n" +
			"             {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n" +
			"             {\"name\": \"age\", \"type\": [\"null\", \"int\"]}]" +
			"}";
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

		// initialize and register external table catalog
		InMemoryExternalCatalog catalog = new InMemoryExternalCatalog("kafka_db");
		tblEnv.registerExternalCatalog("dw", catalog);

		// init table source and sink
        initializeTableSource(catalog);
		initializeTableSink(catalog);

		insertBySQL(tblEnv);
		insertByAPI(tblEnv);

		env.execute("Flink Streaming Java API Skeleton");
	}

	static void insertBySQL(TableEnvironment tblEnv) {
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
		kafkaProps.put("group.id", "jerryConsumer");

		// Approach 1: Create sink directly and register sink
//		Kafka011TableSink sink = new Kafka011TableSink(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(OUTPUT_AVRO_SCHEMA)),
//				OUTPUT_TOPIC, kafkaProps, Optional.of(new FlinkFixedPartitioner<>()), new AvroRowSerializationSchema(OUTPUT_AVRO_SCHEMA));
//		tblEnv.registerTableSink("dw.output", sink);
//		tblEnv.sqlUpdate("insert into `dw.output` SELECT id,name,age from dw.test where event['eventTag'] = '10004'");

		// Approach 2: Get sink from ExternalCatalog and register sink
		//ExternalCatalogTable table = tblEnv.getRegisteredExternalCatalog("dw").getTable("output");
		//tblEnv.registerTableSink("dw.output", TableFactoryUtil.findAndCreateTableSink(tblEnv, table));
		tblEnv.sqlUpdate("insert into `dw.output` SELECT id,name,age from dw.test where event['eventTag'] = '10004'");
	}

	static void insertByAPI(TableEnvironment tblEnv) {
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
		kafkaProps.put("group.id", "jerryConsumer");

		// Approach 1: Create sink directly and register sink
//		Table result = tblEnv.sqlQuery("SELECT id,name,age from dw.test where event['eventTag'] = '10004'");
//		Kafka011TableSink sink = new Kafka011TableSink(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(OUTPUT_AVRO_SCHEMA)),
//				OUTPUT_TOPIC, kafkaProps, Optional.of(new FlinkFixedPartitioner<>()), new AvroRowSerializationSchema(OUTPUT_AVRO_SCHEMA));
//		//result.writeToSink(new CsvTableSink("/tmp/jerryjzhang", ","));
//		result.writeToSink(sink);

		// Approach 2: Get sink from ExternalCatalog and register sink
		Table result = tblEnv.sqlQuery("SELECT id,name,age from dw.test where event['eventTag'] = '10004'");
		ExternalCatalogTable table = tblEnv.getRegisteredExternalCatalog("dw").getTable("output");
		result.writeToSink(TableFactoryUtil.findAndCreateTableSink(tblEnv, table));
	}

	static void initializeTableSource(InMemoryExternalCatalog catalog){
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
		kafkaProps.put("zookeeper.connect", Zk_CONN_STR);
		kafkaProps.put("group.id", "jerryConsumer");

		// initialize table descriptors
		ConnectorDescriptor connectorDescriptor = new Kafka()
				.version("0.11")
				.topic(INPUT_TOPIC)
				.properties(kafkaProps)
				.startFromEarliest();
		FormatDescriptor formatDescriptor = new Avro().avroSchema(AVRO_SCHEMA);
		Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(AVRO_SCHEMA)));
		// create and register external table
		ExternalCatalogTable kafkaTable = new ExternalCatalogTableBuilder(connectorDescriptor)
				.withFormat(formatDescriptor).withSchema(schemaDesc).inAppendMode().asTableSource();

		catalog.createTable("test", kafkaTable, true);
	}

	static void initializeTableSink(InMemoryExternalCatalog catalog){
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
		kafkaProps.put("zookeeper.connect", Zk_CONN_STR);
		kafkaProps.put("group.id", "jerryConsumer");

		// initialize table descriptors
		ConnectorDescriptor connectorDescriptor = new Kafka()
				.version("0.11")
				.topic(OUTPUT_TOPIC)
				.properties(kafkaProps)
				.startFromEarliest();
		FormatDescriptor formatDescriptor = new Avro().avroSchema(OUTPUT_AVRO_SCHEMA);
		Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(OUTPUT_AVRO_SCHEMA)));
		// create and register external table
		ExternalCatalogTable kafkaTable = new ExternalCatalogTableBuilder(connectorDescriptor)
				.withFormat(formatDescriptor).withSchema(schemaDesc).inAppendMode().asTableSink();

		catalog.createTable("output", kafkaTable, true);
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
}