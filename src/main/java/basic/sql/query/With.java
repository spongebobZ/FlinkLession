package basic.sql.query;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 达到在一个sql查询中复用同一个子查询的效果
 */
public class With {
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
        tableEnvironment.executeSql("with dwd_score as " +
                "(select stu_no, sum(score) as total_score from ods_score group by stu_no) " +
                "select max(total_score) as max_score from dwd_score").print();
    }
}
