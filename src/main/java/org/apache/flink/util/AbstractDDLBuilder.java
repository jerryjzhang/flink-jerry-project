package org.apache.flink.util;

import java.util.List;

public abstract class AbstractDDLBuilder {
    public static final String PROPERTY_COLUMN_DEFITION = "column_definition";
    public static final String PROPERTY_PRIMARY_KEY = "primary_key";
    public static final String PROPERTY_ROWTIME_COLUMN = "rowtime_column";
    public static final String PROPERTY_PROCTIME_COLUMN = "proctime_column";
    public static final String PROPERTY_WATERMARK_INTERVAL = "watermark_interval";
    public static final String PROPERTY_SINK_PARALLELISM = "sink_parallelism";
    public static final String PROPERTY_INCLUDED_COLUMNS = "included_columns";

    public abstract String getDDLTableName(String database, String table);
    public abstract String getDDLString(String database, String table, DDLContext ctx);

    protected void validParameters(String... parameters) {
        for (String p : parameters) {
            assert p != null;
        }
    }

    public static class DDLContext {
        public String columnDef;
        public String keyCol;
        public String rowTimeCol;
        public String procTimeCol;
        public Integer watermarkInterval;
        public Integer parallelism = 1;
        public List<String> includeColumns;
        public String kafkaTopic;

        public static DDLContext EMPTY = new DDLContext();

        public DDLContext columnDef(String columnDef) {
            this.columnDef = columnDef;
            return this;
        }
        public DDLContext keyCol(String keyCol) {
            this.keyCol = keyCol;
            return this;
        }
        public DDLContext rowTimeCol(String rowTimeCol) {
            this.rowTimeCol = rowTimeCol;
            return this;
        }
        public DDLContext watermarkInterval(Integer watermarkInterval) {
            this.watermarkInterval = watermarkInterval;
            return this;
        }
        public DDLContext procTimeCol(String procTimeCol) {
            this.procTimeCol = procTimeCol;
            return this;
        }
        public DDLContext parallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }
        public DDLContext includedColumns(List<String> includeColumns) {
            this.includeColumns = includeColumns;
            return this;
        }
        public DDLContext kafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
            return this;
        }
    }
}
