package basic.sql.batch;

import basic.sql.util.TableUtil;
import conf.MysqlConf;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 输出数据到数据库
 */
public class WriteTable {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getBatchTableEnv();

        String ddl = "create table score(" +
                "stuname string," +
                "subject string," +
                "score int," +
                "primary key(stuname) not enforced" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'url' = '" + MysqlConf.URL + "'," +
                "'username' = '" + MysqlConf.USERNAME + "'," +
                "'password' = '" + MysqlConf.PASSWORD + "'," +
                "'table-name' = 'score'" +
                ")";

        tableEnv.executeSql(ddl);

        tableEnv.executeSql("insert into score(stuname, subject, score) " +
                "values ('jenny','english',78), ('tom', 'chinese', 90)");
    }
}
