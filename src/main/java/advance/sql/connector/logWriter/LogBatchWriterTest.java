package advance.sql.connector.logWriter;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

public class LogBatchWriterTest {
    public static void main(String[] args) {
        int parallelism = 2;
        TableEnvironment tableEnv = TableUtil.getBatchTableEnv(parallelism);

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int," +
                "pt as proctime()" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'batch'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score.csv'," +
                "'separator' = ','," +
                "'parallelism' = '" + parallelism + "'" +
                ")";

        String dimStudentDdl = "create table dim_student(" +
                "stu_no int," +
                "stu_name varchar," +
                "age int" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/dim_student.csv'," +
                "'separator' = ','" +
                ")";

        String dwdScoreDdl = "create table dwd_score(" +
                "stu_no int," +
                "stu_name varchar," +
                "age int," +
                "sub_no int," +
                "score int," +
                "primary key(stu_no) not enforced" +
                ") with (" +
                "'connector' = 'log-writer'," +
                "'mode' = 'batch'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logWriter/dwd_score'," +
                "'separator' = ','," +
                "'parallelism' = '" + parallelism + "'" +
                ")";

        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql(dimStudentDdl);
        tableEnv.executeSql(dwdScoreDdl);

        tableEnv.executeSql("insert into dwd_score(stu_no, stu_name, age, sub_no, score)" +
                " select os.stu_no, stu_name, age, sub_no, score from ods_score os " +
                " left join dim_student for system_time as of os.pt as ds " +
                "on os.stu_no = ds.stu_no");
    }
}
