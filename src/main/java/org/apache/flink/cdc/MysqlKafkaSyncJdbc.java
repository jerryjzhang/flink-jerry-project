package org.apache.flink.cdc;

import org.apache.flink.util.FlinkSyncJob;

public class MysqlKafkaSyncJdbc {
    public static void main(String [] args) throws Exception {
        FlinkSyncJob syncJob = new FlinkSyncJob(new FlinkSyncJob.SyncContext()
                .jdbcHost("localhost")
                .jdbcPort("3306")
                .jdbcUsername("jerryjzhang")
                .jdbcPassword("tme")
                .kafkaServers("localhost:9092"));

        syncJob.kafka2Jdbc("jerry", "products",
                "jerry", "products_sink", 1);
        syncJob.run();
    }
}
