package basic.sql.batch;

import conf.MysqlConf;
import org.apache.flink.table.api.TableEnvironment;
import basic.sql.util.TableUtil;

public class Calculate {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getBatchTableEnv();

        String scoreDdl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'url' = '" + MysqlConf.URL + "'," +
                "'username' = '" + MysqlConf.USERNAME + "'," +
                "'password' = '" + MysqlConf.PASSWORD + "'," +
                "'table-name' = 'score'" +
                ")";

        String studentDdl = "create table student(" +
                "stuname string," +
                "class string," +
                "gender int," +
                "age int," +
                "city string" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'url' = '" + MysqlConf.URL + "'," +
                "'username' = '" + MysqlConf.USERNAME + "'," +
                "'password' = '" + MysqlConf.PASSWORD + "'," +
                "'table-name' = 'student'" +
                ")";

        String dml = "select t1.stuname, t1.subject, t1.score, t2.class, t2.gender, t2.age, t2.city " +
                "from score t1 " +
                "inner join student t2 " +
                "on t1.stuname = t2.stuname";

        tableEnv.executeSql(scoreDdl);
        tableEnv.executeSql(studentDdl);
        tableEnv.executeSql(dml).print();
    }
}
