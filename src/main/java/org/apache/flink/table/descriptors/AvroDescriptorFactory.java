package org.apache.flink.table.descriptors;

import org.apache.flink.table.factories.FormatDescriptorFactory;

import java.util.List;
import java.util.Map;

public class AvroDescriptorFactory implements FormatDescriptorFactory {
    @Override
    public FormatDescriptor createFormatDescriptor(Map<String, String> properties) {
        return null;
    }

    @Override
    public Map<String, String> requiredContext() {
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }
}
