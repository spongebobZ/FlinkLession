package basic.sql.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.util.Map;

public class TableManager {
    private final TableEnvironment tableEnvironment;

    public TableManager(String tableEnvType, int parallelism){
        if (tableEnvType.equalsIgnoreCase("stream")){
            StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            streamExecutionEnvironment.setParallelism(parallelism);
            tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        }else{
            tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        }
    }

    public void createDbzTable(String tableName, Map<String, String> schema){
        StringBuilder stringBuilder = new StringBuilder("create table " + tableName + " (");
        for (Map.Entry<String, String> entry: schema.entrySet()){
            stringBuilder.append(entry.getKey()).append(" ").append(entry.getValue()).append(",");
        }
        stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length())
                .append(") with (");
        stringBuilder.append("'connector'='kafka',")
                .append("topic' = 'test','")
                .append("'properties.bootstrap.servers' = 'master:9092',")
                .append("'properties.group.id' = 'test'")
                .append("'scan.startup.mode' = 'latest-offset',")
                .append("'format'='debezium-json'");
        tableEnvironment.executeSql(stringBuilder.toString());
    }

    public void registerUDF(String functionName, Class<? extends UserDefinedFunction> clszz){
        tableEnvironment.createTemporaryFunction(functionName, clszz);
    }

    public void executeSQL(String sql){
        tableEnvironment.executeSql(sql);
    }
}
