package org.apache.flink.util;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class MyTimeWindow {
    static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

    private String start_time;
    private String end_time;

    public MyTimeWindow(TimeWindow window){
        start_time = sdf.format(new Date(window.getStart()));
        end_time = sdf.format(new Date(window.getEnd()));
    }

    @Override
    public String toString() {
        return "MyTimeWindow{" +
                "start_time='" + start_time + '\'' +
                ", end_time='" + end_time + '\'' +
                '}';
    }
}
