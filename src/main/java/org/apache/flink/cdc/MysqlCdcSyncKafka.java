package org.apache.flink.cdc;

import org.apache.flink.cdc.job.Cdc2KafkaJob;

public class MysqlCdcSyncKafka {
    public static void main(String [] args) throws Exception {
        Cdc2KafkaJob job = new Cdc2KafkaJob("localhost", "3306",
                "jerryjzhang", "tme",
                "localhost:9092");

        job.addSync("jerry", "products", 1);
        job.run();
    }
}
