package basic.sql.udf;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;


/**
 * 计算每个用户的总余额（总余额=总收入-总支出）
 */
public class BasicAggregateFunctionTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String ddl = "create table accounting(" +
                "uid int," +
                "income int," +
                "expense int" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'accounting'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
                ")";
        // 创建源表
        tableEnv.executeSql(ddl);
        // 注册udaf(聚合函数)
        tableEnv.createFunction("calc_total_bal", BasicAggregateFunction.class);
        // 调用udf
        tableEnv.executeSql("select uid, calc_total_bal(income, expense) as total_balance " +
                        "from accounting " +
                        "group by uid")
                .print();
    }
}
