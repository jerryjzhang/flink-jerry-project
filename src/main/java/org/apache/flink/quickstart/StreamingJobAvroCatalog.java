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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalogTableBuilder;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.TableFactoryUtil;

public class StreamingJobAvroCatalog extends BaseStreamingExample {
	private static final String AVRO_SCHEMA = "{\n" +
			"         \"type\": \"record\",\n" +
			"         \"name\": \"SdkLog\",\n" +
			"         \"fields\": [\n" +
			"             {\"name\": \"id\", \"type\": [\"null\", \"int\"]},\n" +
			"             {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n" +
			"             {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n" +
			"             {\"name\":\"event\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}]" +
			"}";
	private static final String OUTPUT_AVRO_SCHEMA = "{\n" +
			"         \"type\": \"record\",\n" +
			"         \"name\": \"SdkLogOutput\",\n" +
			"         \"fields\": [\n" +
			"             {\"name\": \"id\", \"type\": [\"null\", \"int\"]},\n" +
			"             {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n" +
			"             {\"name\": \"age\", \"type\": [\"null\", \"int\"]}]" +
			"}";

	public static void main(String[] args) throws Exception {
		// setup local kafka environment
		setupKafkaEnvironment();

		Configuration config = new Configuration();
		config.setString("metrics.reporter.slf4j.class", "org.apache.flink.metrics.slf4j.Slf4jReporter");
		config.setString("metrics.reporter.slf4j.interval", "15 SECONDS");
		final StreamExecutionEnvironment env = new LocalStreamEnvironment(config);
		final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

		// initialize and register external table catalog
		InMemoryExternalCatalog catalog = new InMemoryExternalCatalog("kafka_db");
		tblEnv.registerExternalCatalog("dw", catalog);

		// init table source and sink
        initializeTableSource(catalog);
		initializeTableSink(catalog);

		insertBySQL(tblEnv);
		insertByAPI(tblEnv);

		env.execute("jerry");
	}

	static void insertBySQL(StreamTableEnvironment tblEnv) {
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

	static void insertByAPI(StreamTableEnvironment tblEnv) {
		// Approach 1: Create sink directly and register sink
//		Table result = tblEnv.sqlQuery("SELECT id,name,age from dw.test where event['eventTag'] = '10004'");
//		StreamTableDescriptor descriptor = tblEnv.connect(new Kafka().version("0.11").topic(OUTPUT_TOPIC).properties(kafkaProps).startFromEarliest())
//				.withFormat(new Avro().avroSchema(OUTPUT_AVRO_SCHEMA))
//				.withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(OUTPUT_AVRO_SCHEMA))))
//				.inAppendMode();
//		descriptor.registerTableSink("output");
//		TableSink sink = TableFactoryUtil.findAndCreateTableSink(tblEnv, descriptor);
//		result.writeToSink(sink);

		// Approach 2: Get sink from ExternalCatalog and register sink
		Table result = tblEnv.sqlQuery("SELECT id,name,age from dw.test where event['eventTag'] = '10004'");
		ExternalCatalogTable table = tblEnv.getRegisteredExternalCatalog("dw").getTable("output");
		result.writeToSink(TableFactoryUtil.findAndCreateTableSink(tblEnv, table));
	}

	static void initializeTableSource(InMemoryExternalCatalog catalog){
		// initialize table descriptors
		ConnectorDescriptor connectorDescriptor = new Kafka()
				.version("0.11")
				.topic(AVRO_INPUT_TOPIC)
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
}