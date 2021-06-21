package org.apache.flink.cdc.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class AbstractSyncJob {
    protected final StatementSet syncStatementSet;
    protected final StreamExecutionEnvironment env;
    protected final StreamTableEnvironment tEnv;

    public AbstractSyncJob() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        syncStatementSet = tEnv.createStatementSet();
    }

    public void run() {
        syncStatementSet.execute();
    }

    protected void validParameters(String... parameters) {
        for (String p : parameters) {
            assert p != null;
        }
    }
}
