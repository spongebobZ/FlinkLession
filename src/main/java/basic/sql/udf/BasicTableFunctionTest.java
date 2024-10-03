package basic.sql.udf;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;


/**
 * 查询每个用户的收入和支出
 */
public class BasicTableFunctionTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv();

        String ddl = "create table accounting(" +
                "uid int not null" +
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
        // 注册udtf(表函数)
        tableEnv.createFunction("get_detail", BasicTableFunction.class);
        // 调用udtf
        tableEnv.executeSql("select uid, income, expense from accounting " +
                        "left join " +
                        "lateral table(get_detail(uid)) as t(income, expense) " +
                        "on true")
                .print();
    }
}
