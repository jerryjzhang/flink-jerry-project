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
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;

public class StreamingJobCsv extends BaseStreamingExample {
	public static void main(String[] args) throws Exception {
		// setup local kafka environment
		setupKafkaEnvironment();

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

		// kafka input
		tblEnv.connect(new Kafka().version("0.10")
				.topic(CSV_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
				.withFormat(new Csv().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
				.withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
				.inAppendMode()
				.registerTableSource("test");

		tblEnv.registerFunction("doubleFunc", new DoubleInt());

		// actual sql query
		Table result = tblEnv.sqlQuery("SELECT id,name,doubleFunc(age) from test where event['eventTag'] = '10004' ");
		TableSchema schema = new TableSchema(new String[]{"id", "name", "age"},
				new TypeInformation[]{Types.INT, Types.STRING, Types.INT});
		result.writeToSink(new TestAppendSink(schema));
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class DoubleInt extends ScalarFunction {
		public int eval(int s) {
			return s * 2;
		}
	}
}
