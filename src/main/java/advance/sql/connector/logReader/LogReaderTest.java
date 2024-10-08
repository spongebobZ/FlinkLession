package advance.sql.connector.logReader;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

public class LogReaderTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getBatchTableEnv(2);

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'batch'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score.csv'," +
                "'separator' = ','," +
                "'parallelism' = '2'" +
                ")";

        tableEnv.executeSql(odsScoreDdl);

//        tableEnv.executeSql("select stu_no, sub_no, score from ods_score " +
//                "where stu_no = 1 and sub_no <> 1").print();
        System.out.println(tableEnv.explainSql("select stu_no, sub_no, score from ods_score " +
                "where stu_no <> 1 and sub_no <> 1"));
    }
}
