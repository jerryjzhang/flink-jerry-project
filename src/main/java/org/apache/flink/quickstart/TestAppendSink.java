package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class TestAppendSink implements AppendStreamTableSink<Row> {
    private TableSchema schema;

    public TestAppendSink(TableSchema tableSchema){
        this.schema = tableSchema;
    }

    @Override
    public void emitDataStream(DataStream dataStream) {
        dataStream.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.err.println("DataStream: " + value);
            }
        });
    }

    @Override
    public TypeInformation getOutputType() {
        return schema.toRowType();
    }

    @Override
    public String[] getFieldNames() {
        return new String[0];
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation[0];
    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }
}
