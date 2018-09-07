package org.apache.flink.table.catalog;

import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.ConnectorDescriptorFactory;
import org.apache.flink.table.factories.FormatDescriptorFactory;
import org.apache.flink.table.factories.TableFactoryService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OStreamTableCatalog implements ExternalCatalog {
    private Map<String, Map<String, String>> table2connect = new HashMap<>();
    private Map<String, Map<String, String>> table2format = new HashMap<>();

    public void createTable(String tableName, Map<String, String> connect_params,
                            Map<String, String> format_params) {
        table2connect.put(tableName, connect_params);
        table2format.put(tableName, format_params);
    }

    @Override
    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
        Map<String, String> connect_params = table2connect.get(tableName);
        Map<String, String> format_params = table2format.get(tableName);
        if(connect_params == null || format_params == null) {
            throw new TableNotExistException("dw", tableName);
        }

        ConnectorDescriptor connectorDescriptor = TableFactoryService.find(
                ConnectorDescriptorFactory.class, connect_params)
                .createConnectorDescriptor(connect_params);

        FormatDescriptor formatDescriptor = TableFactoryService.find(
                FormatDescriptorFactory.class, format_params)
                .createFormatDescriptor(format_params);

        Schema schema = TableFactoryService.find(FormatDescriptorFactory.class, format_params)
                .createSchemaDescriptor(format_params);

        return ExternalCatalogTable.builder(connectorDescriptor).inAppendMode()
                    .withSchema(schema)
                    .withFormat(formatDescriptor)
                    .asTableSourceAndSink();
    }

    @Override
    public List<String> listTables() {
        return null;
    }

    @Override
    public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
        throw new CatalogNotExistException(dbName);
    }

    @Override
    public List<String> listSubCatalogs() {
        return new ArrayList<>();
    }
}
