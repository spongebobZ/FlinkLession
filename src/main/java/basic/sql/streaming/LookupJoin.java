package basic.sql.streaming;

import basic.sql.util.TableUtil;
import conf.MysqlConf;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 1. 要求驱动表为流表且含有process-time语义的字段，被关联表为lookup表
 * 2. lookup表的数据从外部查找，lookup表的定义需要表的连接器支持，官方的jdbc连接器支持lookup模式，也可以自定义连接器实现lookup模式
 * 3. 驱动表有数据流入时会触发lookup join，根据join条件从外部数据来查找得到关联的数据(主动触发)
 * 4. lookup表所链接的外部数据有更新时不触发join(被动触动)
 * 5. lookup表可以设置缓存策略, 从外部关联得到的数据能缓存起来, 后续查找时优先从缓存查找，没命中时才去外部查找
 * 6. lookup join保存的状态可以比双流join更少，很多情况下性能比双流join更好，需要的内存也更少
 */
public class LookupJoin {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int," +
                "pt as proctime()" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'ods_score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
                ")";

        /*
        相关配置项:
        lookup.cache: 配置此项后即声明表属性为lookup表，可选值NONE和PARTIAL，NONE表示lookup表不缓存外部数据，PARTIAL表示开启缓存
        lookup.cache.max-rows: 缓存的最大行数，超过后会删除最早的缓存数据(FIFO)以腾出空间来缓存新行，用来控制缓存大小
        lookup.partial-cache.expire-after-write: 每条外部数据写入缓存后的过期时间，用来控制缓存大小和及时同步外部数据
        lookup.partial-cache.expire-after-access: 每条缓存的数据，多久没有被关联上后的过期时间，用来控制缓存大小和及时同步外部数据
        lookup.max-retries: 查询外部数据出现异常失败后的重试次数，默认3次
         */
        String dimStudentDdl = "create table dim_student(" +
                "stu_no int," +
                "stu_name string" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'driver' = '" + MysqlConf.DRIVER + "'," +
                "'url' = '" + MysqlConf.URL + "'," +
                "'username' = '" + MysqlConf.USERNAME + "'," +
                "'password' = '" + MysqlConf.PASSWORD + "'," +
                "'table-name' = 'dim_student'," +
                "'lookup.cache' = 'PARTIAL'," +
                "'lookup.partial-cache.expire-after-write' = '30s'" +
                ")";

        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql(dimStudentDdl);

        tableEnv.executeSql("select os.stu_no, stu_name, sub_no, score from ods_score os " +
                "left join dim_student for system_time as of os.pt as ds " +
                "on os.stu_no = ds.stu_no").print();
    }
}
