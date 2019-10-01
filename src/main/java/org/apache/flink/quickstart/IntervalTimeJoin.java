package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class IntervalTimeJoin {
    static List<Tuple4<Integer, String, Integer, Timestamp>> download = new ArrayList<>();
    static List<Tuple4<Integer, String, Integer, Timestamp>> activate = new ArrayList<>();

    static {
        download.add(new Tuple4<>(1, "yangguo", 30,   Timestamp.valueOf("2019-10-01 11:48:00")));
        download.add(new Tuple4<>(1, "yangguo", 40,   Timestamp.valueOf("2019-10-01 11:49:00")));
        download.add(new Tuple4<>(1, "yangguo", 50,   Timestamp.valueOf("2019-10-01 11:50:00")));

        activate.add(new Tuple4<>(1, "yangguo", 90,   Timestamp.valueOf("2019-10-01 11:50:00")));
        activate.add(new Tuple4<>(1, "yangguo", 95,   Timestamp.valueOf("2019-10-01 11:51:00")));
        activate.add(new Tuple4<>(1, "yangguo", 100,  Timestamp.valueOf("2019-10-01 11:52:00")));
    }

    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream downloadSrc = env.fromCollection(download).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, String, Integer, Timestamp>>(Time.hours(1)) {
            @Override
            public long extractTimestamp(Tuple4<Integer, String, Integer, Timestamp> o) {
                return o.f3.getTime();
            }
        });
        tblEnv.registerDataStream("download", downloadSrc, "id, name, age, downloadTime.rowtime");

        DataStream activateSrc = env.fromCollection(activate).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, String, Integer, Timestamp>>(Time.hours(1)) {
                    @Override
                    public long extractTimestamp(Tuple4<Integer, String, Integer, Timestamp> o) {
                        return o.f3.getTime();
                    }
                });
        tblEnv.registerDataStream("activate", activateSrc, "id, name, grade, activateTime.rowtime");

        Table sinkTable = tblEnv.sqlQuery("SELECT p.name, p.age, b.grade, cast(b.activateTime as timestamp) as aTime, p.downloadTime" +
                " FROM download p, activate b WHERE p.id = b.id AND p.name = b.name " +
                " AND p.downloadTime BETWEEN b.activateTime - INTERVAL '1' MINUTE AND b.activateTime");

        TableSchema schema = new TableSchema(new String[]{"name", "age", "grade", "dtime", "atime"},
                new TypeInformation[]{Types.STRING, Types.INT, Types.INT,
                        Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP});
        sinkTable.writeToSink(new TestAppendSink(schema));

        env.execute();
    }
}
