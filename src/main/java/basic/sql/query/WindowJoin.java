package basic.sql.query;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 双流基于处理窗口进行关联，通常是关联同一个窗口内的数据（根据window_start, window_end字段和业务字段去关联）
 * 两侧数据流均结束对应时间窗口后才会触发该窗口的关联
 */
public class WindowJoin {
    public static void main(String[] args) {
        int parallelism = 2;
        TableEnvironment tableEnvironment = TableUtil.getStreamTableEnv(parallelism);


        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int," +
                "ts timestamp(0)," +
                "watermark for ts as ts" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'stream'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/files/score/ods_score.csv'," +
                "'separator' = ','," +
                "'parallelism' = '" + parallelism + "'" +
                ")";

        String odsCheckInDdl = "create table ods_check_in(" +
                "stu_no int," +
                "check_in_status varchar(16)," +
                "ts timestamp(0)," +
                "watermark for ts as ts" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'stream'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/files/check_in/ods_check_in.csv'," +
                "'separator' = ','," +
                "'parallelism' = '" + parallelism + "'" +
                ")";

        String queryDml = "select * from (select stu_no, sub_no, score, window_start, window_end " +
                "from table(tumble(table ods_score, descriptor(ts), interval '1' hour))) t1 " +
                "left join " +
                "(select stu_no, check_in_status, ts, window_start, window_end " +
                "from table(tumble(table ods_check_in, descriptor(ts), interval '1' hour))) t2 " +
                "on t1.window_start=t2.window_start and t1.window_end=t2.window_end and t1.stu_no=t2.stu_no";
        tableEnvironment.executeSql(odsScoreDdl);
        tableEnvironment.executeSql(odsCheckInDdl);
        tableEnvironment.executeSql(queryDml).print();
    }
}
