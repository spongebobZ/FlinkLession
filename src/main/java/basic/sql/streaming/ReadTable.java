package basic.sql.streaming;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 实时读取kafka的数据并打印到屏幕
 */
public class ReadTable {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String ddl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'study'," +
                "'properties.bootstrap.servers' = 'master:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql("select stuname, subject, score from score").print();
    }
}
