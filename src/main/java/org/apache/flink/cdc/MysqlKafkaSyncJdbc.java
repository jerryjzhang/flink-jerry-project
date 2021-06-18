package org.apache.flink.cdc;

import org.apache.flink.cdc.job.Kafka2JdbcJob;

public class MysqlKafkaSyncJdbc {
    public static void main(String [] args) throws Exception {
        Kafka2JdbcJob job = new Kafka2JdbcJob("localhost", "3306",
                "jerryjzhang", "tme", "localhost:9092", "cmsSyncGroup");

        job.addSync("jerry", "products",
                "jerry", "products_sink", 1);
        job.run();
    }
}
