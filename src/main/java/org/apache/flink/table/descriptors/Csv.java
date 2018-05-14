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

package org.apache.flink.table.descriptors;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.typeutils.TypeStringUtils;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.descriptors.CsvValidator.*;

/**
  * Format descriptor for JSON.
  */
public class Csv extends FormatDescriptor {

	private String schema;
	private String fieldDelimiter;
	private String mapEntryDelimiter;
	private String mapKeyDelimiter;

	public Csv schema(TypeInformation<?> schemaType){
		Preconditions.checkNotNull(schemaType);
		this.schema = TypeStringUtils.writeTypeInfo(schemaType);

		return this;
	}

	public Csv fieldDelimiter(String fieldDelimiter){
		this.fieldDelimiter = fieldDelimiter;
		return this;
	}

	public Csv mapEntryDelimter(String mapEntryDelimiter){
		this.mapEntryDelimiter = mapEntryDelimiter;
		return this;
	}

	public Csv mapKeyDelimiter(String mapKeyDelimiter){
		this.mapKeyDelimiter = mapKeyDelimiter;
		return this;
	}

	/**
	  * Format descriptor for JSON.
	  */
	public Csv() {
		super(FORMAT_TYPE_VALUE, 1);
	}


	/**
	 * Internal method for format properties conversion.
	 */
	@Override
	public void addFormatProperties(DescriptorProperties properties) {
		if (schema != null) {
			properties.putString(FORMAT_SCHEMA, schema);
		}

		if(fieldDelimiter != null) {
			properties.putString(FORMAT_CSV_FIELD_DELIMITER, fieldDelimiter);
		}

		if(mapEntryDelimiter != null) {
			properties.putString(FORMAT_CSV_MAP_ENTRY_DELIMITER, mapEntryDelimiter);
		}

		if(mapKeyDelimiter != null) {
			properties.putString(FORMAT_CSV_MAP_KEY_DELIMITER, mapKeyDelimiter);
		}
	}
}
