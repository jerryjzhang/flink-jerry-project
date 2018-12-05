package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvRowFormatFactory  implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row>  {
    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> context = new HashMap<>();
        context.put(FormatDescriptorValidator.FORMAT_TYPE, CsvValidator.FORMAT_TYPE_VALUE());
        context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public boolean supportsSchemaDerivation() {
        return false;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();
        properties.addAll(SchemaValidator.getSchemaDerivationKeys());
        properties.add("format.fields.#.name");
        properties.add("format.fields.#.type");
        properties.add("format.field-delimiter");
        return properties;
    }

    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = validateAndGetProperties(properties);

        // create and configure
        final CsvRowDeserializationSchema schema = new CsvRowDeserializationSchema(createTypeInformation(descriptorProperties));

        return schema;
    }

    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = validateAndGetProperties(properties);

        // create and configure
        return new JsonRowSerializationSchema(createTypeInformation(descriptorProperties));
    }

    private static DescriptorProperties validateAndGetProperties(Map<String, String> propertiesMap) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(propertiesMap);

        // validate
        //new JsonValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    private static TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
        TableSchema schema = descriptorProperties.getTableSchema(CsvValidator.FORMAT_FIELDS());
        return new RowTypeInfo(schema.getTypes(), schema.getColumnNames());
    }
}
