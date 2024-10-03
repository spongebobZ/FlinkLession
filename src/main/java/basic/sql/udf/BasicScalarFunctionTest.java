package basic.sql.udf;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;


/**
 * 计算每个用户的余额（余额=收入-支出）
 */
public class BasicScalarFunctionTest {
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
        // 注册udf(标量函数)
        tableEnv.createFunction("calc_bal", BasicScalarFunction.class);
        // 调用udf
        tableEnv.executeSql("select uid, calc_bal(income, expense) as balance from accounting")
                .print();
    }
}
