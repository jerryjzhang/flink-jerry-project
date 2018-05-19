package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Created by jerryjzhang on 18/5/19.
 */
public class PrintStreamTableSink implements AppendStreamTableSink<Row> {
    private String [] fieldNames;
    private TypeInformation[] fieldTypes;
    private TypeInformation<Row> returnTypes;

    public PrintStreamTableSink(TypeInformation<Row> returnTypes){
        this.returnTypes = returnTypes;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.print();
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return returnTypes;
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }
}
