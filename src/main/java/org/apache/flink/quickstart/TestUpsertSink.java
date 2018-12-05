package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

public class TestUpsertSink implements UpsertStreamTableSink<Row> {
    private TableSchema schema;

    public TestUpsertSink(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public void setKeyFields(String[] keys) {

    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {

    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return schema.toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        dataStream.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.err.println("DataStream: " + value);
            }
        });
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, schema.toRowType());
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
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }
}
