package basic.sql.query;

import basic.sql.util.TableUtil;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 流式基于over的窗口函数使用
 * 窗口定义必须基于时间语义字段进行排序且必须为升序
 * 统计结果更新不是以回撤流的方式，而是以追加流的方式输出（输出insert类型的数据）
 */
public class Over {
    public static void main(String[] args) {
        int parallelism = 2;
        TableEnvironment tableEnvironment = TableUtil.getStreamTableEnv(parallelism);


        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'stream'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score.csv'," +
                "'separator' = ','," +
                "'parallelism' = '" + parallelism + "'" +
                ")";
        tableEnvironment.executeSql(odsScoreDdl);
        tableEnvironment.executeSql("select stu_no, " +
                "sum(score) over (partition by stu_no order by proctime() asc) as max_score from ods_score").print();
    }
}
