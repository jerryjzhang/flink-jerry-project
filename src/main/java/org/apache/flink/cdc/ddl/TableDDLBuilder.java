package org.apache.flink.cdc.ddl;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public interface TableDDLBuilder {
    ConfigOption<Integer> OPTION_SINK_PARAMETER =
            ConfigOptions.key("sink.parallelism").defaultValue(1);

    String getDDLTableName(String database, String table);
    String getDDLString(String database, String table, String columnDef, Configuration options);
}
