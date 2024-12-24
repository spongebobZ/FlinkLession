package advance.sql.connector.logReader;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

public class LogReaderLookupTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getBatchTableEnv(2);

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int," +
                "pt as proctime()" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'batch'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score_81920.csv'," +
                "'separator' = ','," +
                "'parallelism' = '2'" +
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

        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql(dimStudentDdl);

        tableEnv.executeSql("select os.stu_no, stu_name, age, sub_no, score from ods_score os " +
                " join dim_student for system_time as of os.pt as ds " +
                "on os.stu_no = ds.stu_no").print();
    }
}
