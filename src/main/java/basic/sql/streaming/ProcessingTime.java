package basic.sql.streaming;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 定义processing time
 */
public class ProcessingTime {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String ddl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int," +
                "ts as proctime()" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

        String sql = "select stuname, subject, score, ts from score";
//        String sql = "select stuname, subject, score, proctime() as ts from score";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql(sql).print();
    }
}
