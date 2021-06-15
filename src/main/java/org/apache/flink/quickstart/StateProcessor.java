package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

public class StateProcessor {
    public static void main(String [] args) throws Exception {
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv,
                "file:////tmp/checkpoints/5fef484c8e4a1109bed708d7c61867f5/chk-5",
                new HashMapStateBackend());
        OperatorID id = new OperatorID();
        DataSet<byte[]> listState = savepoint.readUnionState("6cdc5bb954874d922eaee11a8e7b5dd5",
                "offset-states",
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        listState.print();
    }
}
