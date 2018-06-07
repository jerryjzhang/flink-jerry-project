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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.jerry.Kafka011AvroTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import scala.Option;
import scala.Some;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJobAvroCatalog {
	private static final String INPUT_TOPIC = "testJerry";
	private static final String KAFKA_CONN_STR = "localhost:5001";
	private static final String Zk_CONN_STR = "localhost:5000";
	private static KafkaUnit kafkaServer = new KafkaUnit(Zk_CONN_STR, KAFKA_CONN_STR);

	public static void main(String[] args) throws Exception {
		// setup local kafka environment
		setupKafkaEnvironment();

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

		initializeTableSource(tblEnv);

		// actual sql query
		Table result = tblEnv.sqlQuery("SELECT * from kafka_db.test");
		// kafka output
		result.writeToSink(new PrintStreamTableSink());

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	static void initializeTableSource(StreamTableEnvironment tblEnv){
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", KAFKA_CONN_STR);
		kafkaProps.put("zookeeper.connect", Zk_CONN_STR);
		kafkaProps.put("group.id", "jerryConsumer");
		// initialize and register external table catalog
		InMemoryExternalCatalog catalog = new InMemoryExternalCatalog("kafka_db");
		tblEnv.registerExternalCatalog("kafka_db", catalog);

		// initialize table descriptors
		ConnectorDescriptor connectorDescriptor = new Kafka()
				.version("jerry")
				.topic(INPUT_TOPIC)
				.properties(kafkaProps)
				.startFromEarliest();
		FormatDescriptor formatDescriptor = new Avro().recordClass(SdkLog.class);
		Schema schemaDesc = new Schema()
				.field("id", Types.INT)
				.field("name", Types.STRING)
				.field("age", Types.INT);
				//.field("event", Types.MAP(Types.STRING, Types.STRING));
		Some<FormatDescriptor> formatDescriptorSome = new Some<>(formatDescriptor);
		Some<Schema> schemaSome = new Some<>(schemaDesc);
		Some<Metadata> metadataSome = new Some<>(new Metadata());
		Option statisticsSome = Option.empty();
		// create and register external table
		ExternalCatalogTable kafkaTable =
				new ExternalCatalogTable(connectorDescriptor, formatDescriptorSome, schemaSome,statisticsSome,metadataSome);
		catalog.createTable("test", kafkaTable, true);
	}

	private static void setupKafkaEnvironment()throws Exception{
		kafkaServer.startup();
		kafkaServer.createTopic(INPUT_TOPIC);

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
		return out.toByteArray();
	}
}
