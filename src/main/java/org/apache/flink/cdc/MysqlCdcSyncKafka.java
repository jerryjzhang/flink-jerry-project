package org.apache.flink.cdc;

import org.apache.flink.util.FlinkSyncJob;

public class MysqlCdcSyncKafka {
    public static void main(String [] args) throws Exception {
        FlinkSyncJob syncJob = new FlinkSyncJob(new FlinkSyncJob.SyncContext()
                .jdbcHost("localhost")
                .jdbcPort("3306")
                .jdbcUsername("jerryjzhang")
                .jdbcPassword("tme")
                .kafkaServers("localhost:9092"));

        syncJob.cdc2kafka("jerry", "products", 1);
        syncJob.run();
    }
}
