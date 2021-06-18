package org.apache.flink.cdc.ddl;

import org.apache.flink.configuration.Configuration;

public class EsTableDDLBuilder implements TableDDLBuilder {
    private final String esHost;
    private final String esIndex;

    public EsTableDDLBuilder(String esHost, String esIndex) {
        this.esHost = esHost;
        this.esIndex = esIndex;
    }

    @Override
    public String getDDLTableName(String database, String table) {
        return database + "." + table + "_es";
    }

    @Override
    public String getDDLString(String database, String table, String columnDef, Configuration props) {
        String tableName = getDDLTableName(database, table);
        return  String.format("CREATE TABLE %s (\n" +
                "%s" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-6',\n" +
                "  'hosts' = '%s',\n" +
                "  'index' = '%s',\n" +
                "  'document-type' = 'cmstype'\n" +
                ")", tableName, columnDef, esHost, esIndex);
    }
}
