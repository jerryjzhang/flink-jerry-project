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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.11.
 */
@PublicEvolving
public class Kafka011CsvTableSource extends KafkaTableSource {
	private TableSchema schema;
	/**
	 * Creates a Kafka 0.11 JSON {@link StreamTableSource}.
	 *
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param tableSchema The schema of the table.
	 */
	public Kafka011CsvTableSource(
		String topic,
		Properties properties,
		TableSchema tableSchema) {

		super(topic, properties, tableSchema, schemaToReturnType(tableSchema));
		this.schema = tableSchema;
	}

	@Override
	protected CsvRowDeserializationSchema getDeserializationSchema() {
		CsvRowDeserializationSchema deserSchema = new CsvRowDeserializationSchema(schemaToReturnType(schema));
		return deserSchema;
	}

	/** Converts the JSON schema into into the return type. */
	private static RowTypeInfo schemaToReturnType(TableSchema jsonSchema) {
		return new RowTypeInfo(jsonSchema.getTypes(), jsonSchema.getColumnNames());
	}

	/**
	 * Declares a field of the schema to be a processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 */
	@Override
	public void setProctimeAttribute(String proctimeAttribute) {
		super.setProctimeAttribute(proctimeAttribute);
	}

	/**
	 * Declares a field of the schema to be a rowtime attribute.
	 *
	 * @param rowtimeAttributeDescriptor The descriptor of the rowtime attribute.
	 */
	public void setRowtimeAttributeDescriptor(RowtimeAttributeDescriptor rowtimeAttributeDescriptor) {
		Preconditions.checkNotNull(rowtimeAttributeDescriptor, "Rowtime attribute descriptor must not be null.");
		super.setRowtimeAttributeDescriptors(Collections.singletonList(rowtimeAttributeDescriptor));
	}

	@Override
	protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
		return new FlinkKafkaConsumer011<>(topic, deserializationSchema, properties);
	}

	/**
	 * Returns a builder to configure and create a {@link Kafka011JsonTableSource}.
	 * @return A builder to configure and create a {@link Kafka011JsonTableSource}.
	 */
	public static Kafka011CsvTableSource.Builder builder() {
		return new Kafka011CsvTableSource.Builder();
	}

	/**
	 * A builder to configure and create a {@link Kafka011JsonTableSource}.
	 */
	public static class Builder extends KafkaTableSource.Builder<Kafka011CsvTableSource, Kafka011CsvTableSource.Builder> {

		@Override
		protected boolean supportsKafkaTimestamps() {
			return true;
		}

		@Override
		protected Kafka011CsvTableSource.Builder builder() {
			return this;
		}

		/**
		 * Builds and configures a {@link Kafka011JsonTableSource}.
		 *
		 * @return A configured {@link Kafka011JsonTableSource}.
		 */
		@Override
		public Kafka011CsvTableSource build() {
			Kafka011CsvTableSource tableSource = new Kafka011CsvTableSource(
				getTopic(),
				getKafkaProps(),
				getTableSchema());
			super.configureTableSource(tableSource);
			return tableSource;
		}
	}
}