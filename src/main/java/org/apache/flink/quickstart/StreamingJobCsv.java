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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.Kafka011CsvTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSink;
import org.apache.flink.table.api.*;

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
public class StreamingJobCsv {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

		// kafka related configs
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("group.id", "jerryConsumer");
		// kafka input
		Kafka011CsvTableSource.Builder builder = Kafka011CsvTableSource.builder();
		Kafka011CsvTableSource kafkaTable = builder.forTopic("testJerry")
				.withSchema(TableSchema.builder()
						.field("id",Types.INT())
						.field("name", Types.STRING())
						.field("age", Types.INT())
						.build())
				.fromGroupOffsets()
				.withKafkaProperties(kafkaProps)
				.build();
		tblEnv.registerTableSource("test", kafkaTable);

		// actual sql query
		Table result = tblEnv.sqlQuery("SELECT name from test");

		// kafka output
		KafkaJsonTableSink kafkaSink = new Kafka010JsonTableSink("output", kafkaProps);
		result.writeToSink(kafkaSink);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
