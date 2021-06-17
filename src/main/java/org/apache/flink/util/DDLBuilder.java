package org.apache.flink.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public interface DDLBuilder {
    ConfigOption<Integer> OPTION_SINK_PARAMETER =
            ConfigOptions.key("sink.parallelism").defaultValue(1);

    String getDDLTableName(String database, String table);
    String getDDLString(String database, String table, String columnDef, Configuration options);
}
