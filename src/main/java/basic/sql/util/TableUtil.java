package basic.sql.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableUtil {
    public static TableEnvironment getStreamTableEnv(){
        return TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
    }
    public static TableEnvironment getStreamTableEnv(int parallelism){
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tableEnvironment.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", parallelism);
        return tableEnvironment;
    }

    // jobmanager.execution.failover-strategy
    // table.exec.resource.default-parallelism
    public static TableEnvironment getFixedRestartStreamTableEnv(int parallelism, int restartAttempts){
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, 1000));
        streamExecutionEnvironment.enableCheckpointing(3000L);
        streamExecutionEnvironment.disableOperatorChaining();
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
        streamExecutionEnvironment.configure(configuration);
        TableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        tableEnvironment.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", parallelism);
        return tableEnvironment;
    }
    // pipeline.operator-chaining.enabled
    public static TableEnvironment getNoChainingStreamTableEnv(int parallelism){
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.OPERATOR_CHAINING, false);
        streamExecutionEnvironment.configure(configuration);
        streamExecutionEnvironment.setParallelism(parallelism);
        return StreamTableEnvironment.create(streamExecutionEnvironment);
    }

    public static TableEnvironment getUIStreamTableEnv(int parallelism){
        Configuration configuration = new Configuration();
        configuration.setString(WebOptions.LOG_PATH, "/tmp/flink/jobmanager.log");
        configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp/flink/taskmanager.log");
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        streamExecutionEnvironment.setParallelism(parallelism);
        return StreamTableEnvironment.create(streamExecutionEnvironment);
    }

    public static TableEnvironment getBatchTableEnv(){
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    public static TableEnvironment getBatchTableEnv(int parallelism){
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", parallelism);
        return tableEnvironment;
    }
}
