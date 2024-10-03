package basic.sql.streaming;

import org.apache.flink.table.api.TableEnvironment;
import basic.sql.util.TableUtil;

/**
 * 实时排序
 */
public class Ordering {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String ddl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int," +
                "ts timestamp(0)," +
                "watermark for ts as ts - interval '10' second" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

        String sql = "select stuname, subject, score, ts from score order by ts";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql(sql).print();
    }
}

/**
 * 总结:
 * 1. 在流处理模式中，order by排序必须基于flink时间语义字段进行，也就是event time或process time，且必须为asc升序排序
 */
