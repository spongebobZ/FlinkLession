package basic.sql.streaming;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

/**
 * 通过event time定义watermark
 */
public class Watermark {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getNoChainingStreamTableEnv(2);
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        String ddl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int," +
                "ts timestamp_ltz(0)," +
                "watermark for ts as ts" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'score2'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

        String sql = "select stuname, subject, score, ts from score";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql(sql).print();
    }
}
