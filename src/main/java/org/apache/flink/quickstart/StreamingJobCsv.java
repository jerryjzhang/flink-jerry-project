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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class StreamingJobCsv {
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
		tblEnv.connect(new Kafka().version("0.10")
				.topic(INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
				.withFormat(new Csv().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
				.withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
				.inAppendMode()
				.registerTableSource("test");

		tblEnv.registerFunction("doubleFunc", new DoubleInt());

		// actual sql query
		Table result = tblEnv.sqlQuery("SELECT id,name,doubleFunc(age) from test where event['eventTag'] = '10004' ");
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

	private static byte[] generateTestMessage() {
		String message = "1\tjerryjzhang\t32\teventTag:10004,eventId:1000";
		return message.getBytes();
	}

	public static class DoubleInt extends ScalarFunction {
		public int eval(int s) {
			return s * 2;
		}
	}
}
