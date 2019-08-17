package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jerryjzhang on 19/8/17.
 */
public class SingleSourceMultipleSink {
    static List<Tuple3<Integer, String, Integer>> elements = new ArrayList<>();
    static {
        elements.add(new Tuple3<>(1, "yangguo", 30));
        elements.add(new Tuple3<>(2, "huangrong", 40));
        elements.add(new Tuple3<>(3, "guojing", 50));
    }

    public static void main(String [] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        DataStream source = env.fromCollection(elements);
        tblEnv.registerDataStream("test", source, "id,name,age");

        // actual sql query
        TableSchema schema = new TableSchema(new String[]{"id","name", "age"},
                new TypeInformation[]{org.apache.flink.api.common.typeinfo.Types.INT, Types.STRING, Types.INT});
        tblEnv.registerTableSink("output1", new TestAppendSink(schema));
        tblEnv.registerTableSink("output2", new TestAppendSink(schema));

        tblEnv.sqlUpdate("INSERT INTO output1 SELECT id,name,age from test where id = 1");
        tblEnv.sqlUpdate("INSERT INTO output2 SELECT id,name,age from test where id = 2");

        System.out.println(env.getExecutionPlan());

        env.execute();
    }
}
