package org.apache.flink.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Utils {
    private static SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static Timestamp getTime(String time) {
        try {
            return new Timestamp(dtf.parse(time).getTime());
        } catch (ParseException e) {
            return new Timestamp(System.currentTimeMillis());
        }
    }
}
