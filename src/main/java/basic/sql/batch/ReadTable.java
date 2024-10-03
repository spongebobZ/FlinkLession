package basic.sql.batch;

import conf.MysqlConf;
import org.apache.flink.table.api.TableEnvironment;
import basic.sql.util.TableUtil;

/**
 * 读取数据库的表数据并打印到屏幕
 */
public class ReadTable {
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

        tableEnv.executeSql("select stuname, subject, score from score").print();
    }
}
