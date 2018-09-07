package org.apache.flink.table.factories;

import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Schema;

import java.util.Map;

public interface FormatDescriptorFactory extends TableDescriptorFactory {
    FormatDescriptor createFormatDescriptor(Map<String, String> properties);
    Schema createSchemaDescriptor(Map<String, String> properties);
}
