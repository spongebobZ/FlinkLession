package basic.sql.batch;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 在批处理中使用窗口统计
 */
public class BatchWindowProcess {
    public static void main(String[] args) {
        TableEnvironment tableEnvironment = TableUtil.getBatchTableEnv();

        String ddl = "create table payout(" +
                "amount int," +
                "ts timestamp(0)," +
                "watermark for ts as ts" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'batch'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/basic/sql/batch/payout.csv'," +
                "'separator' = ','," +
                "'parallelism' = '2'" +
                ")";

//        String sql = "select * " +
//                "from table(tumble(table income, descriptor(ts), interval '1' hour)) ";

        String sql = "select window_start, window_end, sum(amount) as total_amount " +
                "from table(tumble(table payout, descriptor(ts), interval '1' hour)) " +
                "group by window_start, window_end";

        tableEnvironment.executeSql(ddl);

        tableEnvironment.executeSql(sql).print();
    }
}
