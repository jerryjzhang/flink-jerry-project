package org.apache.flink.table.factories;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.Map;

public interface ConnectorDescriptorFactory extends TableDescriptorFactory {
    ConnectorDescriptor createConnectorDescriptor(Map<String,String> properties);
}
