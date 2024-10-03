package basic.sql.streaming;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 实时统计kafka的数据并打印到屏幕
 */
public class Statistics {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String ddl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql("select stuname, sum(score) as total_score from score " +
                "group by stuname").print();
    }
}
