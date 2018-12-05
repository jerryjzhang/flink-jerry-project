package org.apache.flink.table.descriptors;

import org.apache.flink.table.factories.ConnectorDescriptorFactory;
import org.apache.flink.table.factories.TableFactoryService;

import java.util.*;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.KafkaValidator.*;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_CLASS;

public class KafkaDescriptorFactory implements ConnectorDescriptorFactory {
    @Override
    public ConnectorDescriptor createConnectorDescriptor(Map<String,String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
        final String version = descriptorProperties.getString(CONNECTOR_VERSION);
        final Properties kafkaProperties = new Properties();

        for(Map<String,String> propertyKey : descriptorProperties.getFixedIndexedProperties(
            CONNECTOR_PROPERTIES,
            Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE))) {
            kafkaProperties.put(
                    descriptorProperties.getString(propertyKey.get(CONNECTOR_PROPERTIES_KEY)),
                    descriptorProperties.getString(propertyKey.get(CONNECTOR_PROPERTIES_VALUE))
            );
        }
        ConnectorDescriptor descriptor = new Kafka()
                .version(version)
                .topic(topic)
                .properties(kafkaProperties)
                .startFromGroupOffsets();

        return descriptor;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA); // kafka
        context.put(CONNECTOR_VERSION, "0.10"); // version
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // kafka
        properties.add(CONNECTOR_TOPIC);
        properties.add(CONNECTOR_PROPERTIES);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
        properties.add(CONNECTOR_STARTUP_MODE);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET);
        properties.add(CONNECTOR_SINK_PARTITIONER);
        properties.add(CONNECTOR_SINK_PARTITIONER_CLASS);

        return properties;
    }

    public static void main(String [] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        ConnectorDescriptor descriptor = new Kafka()
                .version("0.10")
                .topic("topic")
                .properties(kafkaProps);

        Map<String,String> properties = descriptor.toProperties();
        descriptor = TableFactoryService.find(ConnectorDescriptorFactory.class, properties)
                .createConnectorDescriptor(properties);
        System.out.println(descriptor.toString());

    }
}
