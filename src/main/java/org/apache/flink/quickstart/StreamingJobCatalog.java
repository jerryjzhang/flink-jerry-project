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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSink;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.descriptors.*;
import scala.Option;
import scala.Some;

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
public class StreamingJobCatalog {

	private static final String JSON_SCHEMA =
			"{" +
					"    'title': 'Person'," +
					"    'type': 'object'," +
					"    'properties': {" +
					"        'id': {" +
					"            'type': 'string'" +
					"        }," +
					"        'name': {" +
					"            'type': 'string'" +
					"        }," +
					"        'age': {" +
					"            'description': 'Age in years'," +
					"            'type': 'number'," +
					"            'minimum': 0" +
					"        }" +
					"    }," +
					"    'required': ['id', 'name']" +
					"}";

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

		initializeTableSource(tblEnv);

		// Approach 1:  use sql "insert into" to write to sink
		initializeTableSink(tblEnv);

		Table srcTbl = tblEnv.scan("kafka_db","test");
		srcTbl.minus()
		tblEnv.sqlUpdate("INSERT INTO testOutput SELECT name FROM kafka_db.test where age > 30");
		tblEnv.sqlUpdate("INSERT INTO testOutput2 SELECT name FROM kafka_db.test where age <= 30");

		// Approach 2: programmatically writes to sink
//		// kafka output
//		// kafka related configs
//		Properties kafkaProps = new Properties();
//		kafkaProps.put("bootstrap.servers", "localhost:9092");
//		kafkaProps.put("group.id", "jerryConsumer");
//		KafkaJsonTableSink kafkaSink = new Kafka010JsonTableSink("output", kafkaProps);
//		// actual sql query
//		Table result = tblEnv.sqlQuery("SELECT name from kafka_db.test");
//		result.writeToSink(kafkaSink);

		env.execute("Flink Streaming Java API Skeleton");
	}

	static void initializeTableSink(StreamTableEnvironment tblEnv){
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		KafkaJsonTableSink kafkaSink = new Kafka010JsonTableSink("testOutput", kafkaProps);
		String[] fieldNames = {"name"};
		TypeInformation[] fieldTypes = {Types.STRING};
		tblEnv.registerTableSink("testOutput", fieldNames, fieldTypes, kafkaSink);

		KafkaJsonTableSink kafkaSink2 = new Kafka010JsonTableSink("testOutput2", kafkaProps);
		tblEnv.registerTableSink("testOutput2", fieldNames, fieldTypes, kafkaSink2);
	}

	static void initializeTableSource(StreamTableEnvironment tblEnv){
		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("group.id", "jerryConsumer");
		// initialize and register external table catalog
		InMemoryExternalCatalog catalog = new InMemoryExternalCatalog("kafka_db");
		tblEnv.registerExternalCatalog("kafka_db", catalog);

		// initialize table descriptors
		ConnectorDescriptor connectorDescriptor = new Kafka()
				.version("0.11")
				.topic("test")
				.properties(kafkaProps)
				.startFromGroupOffsets();
		FormatDescriptor formatDescriptor = new Json().jsonSchema(JSON_SCHEMA);
		Schema schemaDesc = new Schema()
				.field("id", Types.STRING)
				.field("name", Types.STRING)
				.field("age", Types.BIG_DEC);
		Some<FormatDescriptor> formatDescriptorSome = new Some<>(formatDescriptor);
		Some<Schema> schemaSome = new Some<>(schemaDesc);
		Some<Metadata> metadataSome = new Some<>(new Metadata());
		Option statisticsSome = Option.empty();
		// create and register external table
		ExternalCatalogTable kafkaTable =
				new ExternalCatalogTable(connectorDescriptor, formatDescriptorSome, schemaSome,statisticsSome,metadataSome);
		catalog.createTable("test", kafkaTable, true);
	}
}
