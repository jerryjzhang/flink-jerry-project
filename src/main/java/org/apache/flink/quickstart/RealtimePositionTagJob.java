package org.apache.flink.quickstart;

import com.oppo.dc.data.avro.generated.PositionLog;
import com.oppo.dc.data.avro.generated.TagRecord;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class RealtimePositionTagJob extends BaseStreamingExample {
    public static void main(String [] args)throws Exception {
        setupKafkaEnvironment();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                Integer.MAX_VALUE, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        tblEnv.registerFunction("lastPosition", new LastPosition());
        tblEnv.registerFunction("convertPosition", new PositionTagConverter());

        registerSourceTable(tblEnv);
        registerSinkTable(tblEnv);

        tblEnv.sqlUpdate("INSERT INTO userTag " +
                "SELECT tImei,tTagKey,tTagValue FROM " +
                "   (SELECT imei, lastPosition(imei,lat,lon) as pos " +
                "    FROM positionLog GROUP BY imei, TUMBLE(tt, INTERVAL '1' SECOND)), " +
                "LATERAL TABLE(convertPosition(pos)) as T(tImei,tTagKey,tTagValue)");

        env.execute();
    }

    private static void registerSourceTable(StreamTableEnvironment tblEnv) {
        // initialize source tables
        tblEnv.connect(new Kafka().version("0.10")
                .topic(POS_INPUT_TOPIC).properties(kafkaProps).startFromEarliest())
                .withFormat(new Avro().recordClass(PositionLog.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(
                        AvroSchemaConverter.convertToTypeInfo(PositionLog.class)))
                        .field("tt", Types.SQL_TIMESTAMP()).proctime())
                .inAppendMode()
                .registerTableSource("positionLog");
    }

    private static void registerSinkTable(StreamTableEnvironment tblEnv) {
        // initialize sink table
        tblEnv.connect(new Kafka().version("0.10")
                .topic(OUTPUT_TOPIC).properties(kafkaProps))
                .withFormat(new Avro().recordClass(TagRecord.class))
                .withSchema(new Schema().schema(TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(TagRecord.class))))
                .inAppendMode()
                .registerTableSink("userTag");
    }

    public static class LastPosition extends AggregateFunction<PositionLog, PositionLog> {
        @Override
        public PositionLog createAccumulator() {
            return new PositionLog();
        }

        @Override
        public PositionLog getValue(PositionLog accumulator) {
            return accumulator;
        }

        public void accumulate(PositionLog acc, String imei, String lat, String lon) {
            acc.setImei(imei);
            acc.setLat(lat);
            acc.setLon(lon);
        }
    }

    public static class PositionTagConverter extends TableFunction<Tuple3<String, String, Row[]>> {
        private static final String TAG_COUNTRY_KEY = "position.realtime.current_country";
        private static final String TAG_CITY_KEY = "position.realtime.current_city";

        public void eval(PositionLog log) {
            Tuple3 tuple = new Tuple3();
            tuple.f0 = log.getImei();
            tuple.f1 = TAG_COUNTRY_KEY;
            Row[] row = new Row[1];
            row[0] = new Row(3);
            row[0].setField(0, "中国");
            row[0].setField(1, "1");
            tuple.f2 = row;
            collect(tuple);

            Tuple3 tuple2 = new Tuple3();
            tuple2.f0 = log.getImei();
            tuple2.f1 = TAG_CITY_KEY;
            Row[] row2 = new Row[1];
            row2[0] = new Row(3);
            row2[0].setField(0, "深圳市");
            row2[0].setField(1, "1");
            tuple2.f2 = row2;
            collect(tuple2);
        }

        @Override
        public TypeInformation<Tuple3<String, String, Row[]>> getResultType() {
            return org.apache.flink.api.common.typeinfo.Types.TUPLE(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.STRING,
                    org.apache.flink.api.common.typeinfo.Types.OBJECT_ARRAY(org.apache.flink.api.common.typeinfo.Types.ROW(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.STRING)));
        }
    }

}
