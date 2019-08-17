package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jerryjzhang on 19/8/17.
 */
public class SingleSourceMultipleSink extends BaseStreamingExample{
    public static void main(String [] args) throws Exception {
        // setup local kafka environment
        setupKafkaEnvironment();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        dataStreamAPI(env, tblEnv);
        //tableAPI(env, tblEnv);
    }

    private static void dataStreamAPI(StreamExecutionEnvironment env, StreamTableEnvironment tblEnv) {
        DataStream<SdkLog> stream = env.addSource(new FlinkKafkaConsumer011<>(AVRO_INPUT_TOPIC,
                AvroDeserializationSchema.forSpecific(SdkLog.class), kafkaProps).setStartFromEarliest());

        DataStream sdk_feeds = stream.filter(new FilterFunction<SdkLog>() {
            @Override
            public boolean filter(SdkLog sdkLog) throws Exception {
                return sdkLog.getAge() >= 80;
            }
        });

        DataStream sdk_cdo = stream.filter(new FilterFunction<SdkLog>() {
            @Override
            public boolean filter(SdkLog sdkLog) throws Exception {
                return sdkLog.getAge() < 80;
            }
        });

        sdk_feeds.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.err.println("DataStream: " + value);
            }
        });
        sdk_cdo.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.err.println("DataStream: " + value);
            }
        });

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();
        System.out.println(streamGraph.getStreamingPlanAsJSON());
    }

    private static void tableAPI(StreamExecutionEnvironment env, StreamTableEnvironment tblEnv){
        // kafka input
        tblEnv.connect(new Kafka().version("0.10")
                .topic(AVRO_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(SdkLog.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                .inAppendMode()
                .registerTableSource("test");

        // actual sql query
        TableSchema schema = new TableSchema(new String[]{"id","name", "age"},
                new TypeInformation[]{org.apache.flink.api.common.typeinfo.Types.INT, Types.STRING, Types.INT});
        tblEnv.registerTableSink("output1", new TestAppendSink(schema));
        tblEnv.registerTableSink("output2", new TestAppendSink(schema));

        tblEnv.sqlUpdate("INSERT INTO output1 SELECT id,name,age from test where id = 1");
        tblEnv.sqlUpdate("INSERT INTO output2 SELECT id,name,age from test where id = 2");

        StreamGraph streamGraph = env.getStreamGraph();
        convert(streamGraph);
        JobGraph jobGraph = streamGraph.getJobGraph();
        System.out.println(streamGraph.getStreamingPlanAsJSON());
    }

    public static void convert(StreamGraph streamGraph) {
        StreamNode source = streamGraph.getStreamNode(1);
        List<StreamNode> otherSources = new ArrayList<>();
        for(StreamNode node : streamGraph.getStreamNodes()) {
            if(node.getId() > 1 && node.getOperator() instanceof  StreamSource) {
                otherSources.add(node);
                continue;
            }
            List<StreamEdge> edges = node.getInEdges();
            StreamEdge newEdge = null;
            for(StreamEdge edge : edges) {
                if(edge.getSourceVertex().getOperator() instanceof StreamSource){
                    newEdge = new StreamEdge(source, node, edge.getTypeNumber(),
                            edge.getSelectedNames(), edge.getPartitioner(), edge.getOutputTag());
                }
            }
            if(newEdge != null) {
                edges.clear();
                edges.add(newEdge);
            }
        }

        for(StreamNode node : otherSources){
            streamGraph.getStreamNodes().remove(node);
        }

        streamGraph.getSourceIDs().clear();
        streamGraph.getSourceIDs().add(1);
    }
}
