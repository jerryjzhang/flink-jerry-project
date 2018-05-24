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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserialization schema from JSON to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class CsvRowDeserializationSchema implements DeserializationSchema<Row> {
    private static final long serialVersionUID = -228294330688809190L;

	/** Type information describing the result type. */
	private final TypeInformation<Row> typeInfo;
	private String fieldDelimiter = "\t";
	private String mapEntryDelimiter = ",";
	private String mapKeyDelimiter = ":";

	/**
	 * Creates a JSON deserialization schema for the given type information.
	 *
	 * @param typeInfo Type information describing the result type. The field names of {@link Row}
	 *                 are used to parse the JSON properties.
	 */
	public CsvRowDeserializationSchema(TypeInformation<Row> typeInfo) {
		Preconditions.checkNotNull(typeInfo, "Type information");
		this.typeInfo = typeInfo;

		if (!(typeInfo instanceof RowTypeInfo)) {
			throw new IllegalArgumentException("Row type information expected.");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Row deserialize(byte[] message) {
		String msg = new String(message);
		return convertRow(msg, (RowTypeInfo) typeInfo);
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}


	// --------------------------------------------------------------------------------------------

	private Object convert(String field, TypeInformation<?> info) {
		if (info == Types.VOID || field == null) {
			return null;
		} else if (info == Types.BOOLEAN || info.getTypeClass() == Boolean.class) {
			return Boolean.valueOf(field);
		} else if (info == Types.STRING || info.getTypeClass() == String.class) {
			return field;
		} else if (info == Types.BIG_DEC || info.getTypeClass() == BigDecimal.class) {
			return BigDecimal.valueOf(Double.valueOf(field));
		} else if (info == Types.INT || info.getTypeClass() == Integer.class) {
			return Integer.valueOf(field);
		}else if (info == Types.LONG || info.getTypeClass() == Long.class){
			return Long.valueOf(field);
		} else if (info == Types.BIG_INT || info.getTypeClass() == BigInteger.class) {
			return BigInteger.valueOf(Long.valueOf(field));
		} else if (info == Types.SQL_DATE || info.getTypeClass() == Date.class) {
			return Date.valueOf(field);
		} else if (info == Types.SQL_TIME || info.getTypeClass() == Time.class) {
			// according to RFC 3339 every full-time must have a timezone;
			// until we have full timezone support, we only support UTC;
			// users can parse their time as string as a workaround
			final String time = field;
			if (time.indexOf('Z') < 0 || time.indexOf('.') >= 0) {
				throw new IllegalStateException(
					"Invalid time format. Only a time in UTC timezone without milliseconds is supported yet. " +
						"Format: HH:mm:ss'Z'");
			}
			return Time.valueOf(time.substring(0, time.length() - 1));
		} else if (info == Types.SQL_TIMESTAMP || info.getTypeClass() == Timestamp.class) {
			// according to RFC 3339 every date-time must have a timezone;
			// until we have full timezone support, we only support UTC;
			// users can parse their time as string as a workaround
			final String timestamp = field;
			if (timestamp.indexOf('Z') < 0) {
				throw new IllegalStateException(
					"Invalid timestamp format. Only a timestamp in UTC timezone is supported yet. " +
						"Format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			}
			return Timestamp.valueOf(timestamp.substring(0, timestamp.length() - 1).replace('T', ' '));
		} else if (info instanceof MapTypeInfo) {
			return convertMap(field, info);
		} else {
			throw new IllegalStateException("Unsupported type information '" + info + "' for field: " + field);
		}
	}

	private Object convertMap(String line, TypeInformation<?> elementType){
		Map<String, String> mapObj = new HashMap<>();

		String [] entries = line.split(mapEntryDelimiter);
		for(String entry : entries){
			String [] e = entry.split(mapKeyDelimiter);
			String value = "";
			if(e.length > 1){
			    value = e[1];
            }
			mapObj.put(e[0], value);
		}

		return mapObj;
	}

	private Row convertRow(String line, RowTypeInfo info) {
		final String[] names = info.getFieldNames();
		final TypeInformation<?>[] types = info.getFieldTypes();

		String [] csv = line.split(fieldDelimiter);
//		if(csv.length > names.length){
//			throw new IllegalStateException(
//					"Actual #fields is more than that in schema!");
//		}

		final Row row = new Row(names.length);
		for (int i = 0; i < names.length; i++) {
			if(i >= csv.length){
				row.setField(i, null);
			} else {
				row.setField(i, convert(csv[i], types[i]));
			}
		}

		return row;
	}
}
