package org.apache.flink.table.factories;

import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

public interface FormatDescriptorFactory extends TableDescriptorFactory {
    FormatDescriptor createFormatDescriptor(Map<String, String> properties);
}
