package basic.sql.query;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 流式分组统计会不停回撤旧结果，输出新结果
 */
public class Group {
    public static void main(String[] args) {
        int parallelism = 2;
        TableEnvironment tableEnvironment = TableUtil.getStreamTableEnv(parallelism);


        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'stream'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score.csv'," +
                "'separator' = ','," +
                "'parallelism' = '" + parallelism + "'" +
                ")";
        tableEnvironment.executeSql(odsScoreDdl);
        tableEnvironment.executeSql("select stu_no, max(score) as max_score from ods_score " +
                "group by stu_no").print();
    }
}
