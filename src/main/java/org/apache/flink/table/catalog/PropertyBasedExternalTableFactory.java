package org.apache.flink.table.catalog;

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.factories.ConnectorDescriptorFactory;
import org.apache.flink.table.factories.FormatDescriptorFactory;
import org.apache.flink.table.factories.TableFactoryService;

import java.util.Map;

public class PropertyBasedExternalTableFactory {
    public ExternalCatalogTable createExternalCatalogTable(Map<String, String> properties){
        ConnectorDescriptor connectorDescriptor = TableFactoryService
                .find(ConnectorDescriptorFactory.class, properties)
                .createConnectorDescriptor(properties);

        FormatDescriptor formatDescriptor = TableFactoryService
                .find(FormatDescriptorFactory.class, properties)
                .createFormatDescriptor(properties);

        return null;
    }
}
