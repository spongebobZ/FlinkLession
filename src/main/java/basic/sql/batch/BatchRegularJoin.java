package basic.sql.batch;


import basic.sql.util.TableUtil;
import conf.MysqlConf;
import org.apache.flink.table.api.TableEnvironment;

/**
 * batch常规join
 * 1. 支持inner join, left join, right join, full outer join
 * 2. 支持等值条件和非等值条件
 */
public class BatchRegularJoin {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getBatchTableEnv();

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'driver' = '" + MysqlConf.DRIVER + "'," +
                "'url' = '" + MysqlConf.URL + "'," +
                "'username' = '" + MysqlConf.USERNAME + "'," +
                "'password' = '" + MysqlConf.PASSWORD + "'," +
                "'table-name' = 'ods_score'" +
                ")";

        String dimStudentDdl = "create table dim_student(" +
                "stu_no int," +
                "stu_name string" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'driver' = '" + MysqlConf.DRIVER + "'," +
                "'url' = '" + MysqlConf.URL + "'," +
                "'username' = '" + MysqlConf.USERNAME + "'," +
                "'password' = '" + MysqlConf.PASSWORD + "'," +
                "'table-name' = 'dim_student'" +
                ")";


        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql(dimStudentDdl);

        // regular inner join
        tableEnv.executeSql("select os.stu_no, stu_name, sub_no, score from ods_score os " +
                "inner join dim_student ds " +
                "on os.stu_no = ds.stu_no").print();

        // regular left outer join
        tableEnv.executeSql("select os.stu_no, stu_name, sub_no, score from ods_score os " +
                "left join dim_student ds " +
                "on os.stu_no = ds.stu_no").print();
    }
}
