package basic.sql.streaming;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;


/**
 * 基于event time进行滑动窗口统计
 */
public class SlideWindow {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv(1);

        String ddl = "create table income(" +
                "amount int," +
                "ts timestamp(0)," +
                "watermark for ts as ts" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'income'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

        String sql = "select window_start, window_end, sum(amount) as total_amount " +
                "from table(hop(table income, descriptor(ts), interval '10' minutes, interval '1' hour)) " +
                "group by window_start, window_end";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql(sql).print();
    }
}
