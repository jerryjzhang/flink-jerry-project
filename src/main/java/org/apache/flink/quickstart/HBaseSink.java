package org.apache.flink.quickstart;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.generated.SdkLog;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class HBaseSink extends BaseStreamingExample {
    public static void main(String [] args) throws Exception {
        setupKafkaEnvironment();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        // kafka input
        tblEnv.connect(new Kafka().version("0.10")
                .topic(AVRO_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(SdkLog.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(SdkLog.class))))
                .inAppendMode()
                .registerTableSource("test");

        Table result = tblEnv.sqlQuery("SELECT name from test where event['eventTag'] = '10004' ");

        DataStream<String> dataStream = tblEnv.toAppendStream(result, Types.STRING);
        dataStream.writeUsingOutputFormat(new HBaseOutputFormat());

        env.execute();

    }

    /**
     * This class implements an OutputFormat for HBase.
     */
    private static class HBaseOutputFormat implements OutputFormat<String> {

        private org.apache.hadoop.conf.Configuration conf = null;
        private org.apache.hadoop.hbase.client.Table table = null;
        private String taskNumber = null;
        private int rowNumber = 0;

        private static final long serialVersionUID = 1L;

        @Override
        public void configure(org.apache.flink.configuration.Configuration parameters) {
            conf = HBaseConfiguration.create();
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            Connection connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("jerry"));
            this.taskNumber = String.valueOf(taskNumber);
        }

        @Override
        public void writeRecord(String record) throws IOException {
            Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("entry"),
                    Bytes.toBytes(record));
            rowNumber++;
            table.put(put);
        }

        @Override
        public void close() throws IOException {
            table.close();
        }
    }

//
//    private static HBaseTestingUtility createHBaseCluster()throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.regionserver.port", "8909");
//        conf.set("hbase.master.info.port", "8908");
//        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
//        utility.startMiniCluster();
//
//        return utility;
//    }
//
//    private static void hbaseMiniClusterDemo()throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.regionserver.port", "8909");
//        conf.set("hbase.master.info.port", "8908");
//        conf.set("dfs.namenode.name.dir", "file:///tmp/hdfs");
//        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
//        MiniHBaseCluster cluster = utility.startMiniCluster();
//
//        Table table = utility.createTable(TableName.valueOf("MyTable"),
//                Bytes.toBytes("MyColumnFamily"));
//        Put put = new Put(Bytes.toBytes("rowKey"));
//        put.addColumn(Bytes.toBytes("MyColumnFamily"),
//                Bytes.toBytes("name"), Bytes.toBytes("jerry"));
//        table.put(put);
//
//        Get get = new Get(Bytes.toBytes("rowKey"));
//        Result result = table.get(get);
//        System.out.println(new String(result.getValue(Bytes.toBytes("MyColumnFamily"),
//                Bytes.toBytes("name"))));
//    }
}
