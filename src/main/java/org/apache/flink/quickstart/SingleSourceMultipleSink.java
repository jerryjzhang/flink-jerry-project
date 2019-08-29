package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by jerryjzhang on 19/8/17.
 */
public class SingleSourceMultipleSink extends BaseStreamingExample{
    private static final OutputTag<SdkLog> filterTag = new OutputTag<SdkLog>("filterTag") {};

    public static void main(String [] args) throws Exception {
        // setup local kafka environment
        setupKafkaEnvironment();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        //dataStreamAPI(env, tblEnv);
        //tableAPI(env, tblEnv);
        sideOutputAPI(env, tblEnv);
        env.execute();
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

    private static void sideOutputAPI(StreamExecutionEnvironment env, StreamTableEnvironment tblEnv) {
        DataStream<SdkLog> stream = env.addSource(new FlinkKafkaConsumer011<>(AVRO_INPUT_TOPIC,
                AvroDeserializationSchema.forSpecific(SdkLog.class), kafkaProps).setStartFromEarliest());

        SingleOutputStreamOperator result = stream.process(new ProcessFunction<SdkLog, Object>() {
            @Override
            public void processElement(SdkLog value, Context ctx, Collector<Object> out) throws Exception {
                if(value.getAge() < 80) {
                    out.collect(value);
                } else {
                    ctx.output(filterTag, value);
                }
            }
        });

        result.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.err.println("DataStream: " + value);
            }
        });

        DataStream sideOutput = result.getSideOutput(filterTag);
        sideOutput.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.err.println("SideOutput: " + value);
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
        TableSchema schema = new TableSchema(new String[]{"id","name", "age", "event"},
                new TypeInformation[]{org.apache.flink.api.common.typeinfo.Types.INT, Types.STRING, Types.INT, Types.MAP(Types.STRING, Types.STRING)});
        tblEnv.registerTableSink("output1", new TestAppendSink(schema));
        tblEnv.registerTableSink("output2", new TestAppendSink(schema));

        tblEnv.sqlUpdate("INSERT INTO output1 SELECT * from test where id = 1");
        tblEnv.sqlUpdate("INSERT INTO output2 SELECT * from test where id = 2");

        StreamGraph streamGraph = env.getStreamGraph();
        mergeSingleSource(streamGraph);
        JobGraph jobGraph = streamGraph.getJobGraph();
        System.out.println(streamGraph.getStreamingPlanAsJSON());
    }

    static void mergeSingleSource(StreamGraph streamGraph) {
        // assume the single source must be the first stream source
        StreamNode singleSource = null;
        for(StreamNode node : streamGraph.getStreamNodes()) {
            if(node.getOperator() instanceof StreamSource) {
                singleSource = node;
                break;
            }
        }

        assert singleSource != null;

        List<StreamNode> duplicateSources = new ArrayList<>();
        for(StreamNode node : streamGraph.getStreamNodes()) {
            // find out duplicate sources
            if(node.getId() != singleSource.getId() && node.getOperator() instanceof StreamSource &&
                    node.getOperatorName().equals(singleSource.getOperatorName())) {
                duplicateSources.add(node);
                continue;
            }

            List<StreamEdge> edges = node.getInEdges();
            StreamEdge newEdge = null;
            for(StreamEdge edge : edges) {
                // check if a stream node has a source parent
                if(edge.getSourceId() != singleSource.getId() &&
                        streamGraph.getStreamNode(edge.getSourceId()).getOperator() instanceof StreamSource &&
                        streamGraph.getStreamNode(edge.getSourceId()).getOperatorName().
                                equals(singleSource.getOperatorName())) {
                    // instantiate a new inbound stream edge pointing to the single source
                    newEdge = new StreamEdge(singleSource, node, edge.getTypeNumber(),
                            edge.getSelectedNames(), edge.getPartitioner(), edge.getOutputTag());
                }
            }
            if(newEdge != null) {
                edges.clear();
                edges.add(newEdge);
                // instantiate a new outbound stream edge pointing to the single source
                StreamEdge outEdge = new StreamEdge(singleSource, node, newEdge.getTypeNumber(),
                        newEdge.getSelectedNames(), newEdge.getPartitioner(), newEdge.getOutputTag());
                singleSource.addOutEdge(outEdge);
            }
        }

        // remove all duplicate sources
        for(StreamNode node : duplicateSources){
            streamGraph.getStreamNodes().remove(node);
            streamGraph.getSourceIDs().remove(node.getId());
        }
    }
}