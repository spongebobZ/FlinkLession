package basic.sql.streaming;


import basic.sql.util.TableUtil;
import conf.MysqlConf;
import org.apache.flink.table.api.TableEnvironment;

/**
 * stream常规join
 * 1. 支持inner join, left join, right join, full outer join
 * 2. 支持等值条件和非等值条件
 * 3. 默认下join会保存join左右两表旧数据状态, 当新的实时数据进来时, 就会在状态里匹配旧数据,
 * 因此这个状态会随着数据的流入而无限增长, 有可能导致内存溢出, 可以考虑声明配置项'table.exec.state.ttl为xxx'单位是ms,
 * 当某条旧数据超过这个xxx时间都没有被匹配过后, 就会从状态里移除这条旧数据
 */
public class StreamRegularJoin {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

//        tableEnv.getConfig().getConfiguration().setInteger("table.exec.state.ttl", 30000);

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'ods_score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'"+
                ")";

//        String dimStudentDdl = "create table dim_student(" +
//                "stu_no int," +
//                "stu_name string" +
//                ") with (" +
//                "'connector' = 'jdbc'," +
//                "'driver' = '" + MysqlConf.DRIVER + "'," +
//                "'url' = '" + MysqlConf.URL + "'," +
//                "'username' = '" + MysqlConf.USERNAME + "'," +
//                "'password' = '" + MysqlConf.PASSWORD + "'," +
//                "'table-name' = 'dim_student'" +
//                ")";

        String dimStudentDdl = "create table dim_student(" +
                "stu_no int," +
                "stu_name string" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'dim_student'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'debezium-json'"+
                ")";


        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql(dimStudentDdl);

//        // regular inner join
//        tableEnv.executeSql("select os.stu_no, stu_name, sub_no, score from ods_score os " +
//                "inner join dim_student ds " +
//                "on os.stu_no = ds.stu_no").print();

        // regular left outer join
        tableEnv.executeSql("select os.stu_no, stu_name, sub_no, score from ods_score os " +
                "left join dim_student ds " +
                "on os.stu_no = ds.stu_no").print();
    }
}
