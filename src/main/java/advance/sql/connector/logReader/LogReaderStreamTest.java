package advance.sql.connector.logReader;

import basic.sql.util.TableUtil;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.lang.reflect.Field;
import java.time.LocalDateTime;

public class LogReaderStreamTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getStreamTableEnv(2);

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int not null" +
                ") with (" +
                "'connector' = 'log-reader'," +
                "'mode' = 'stream'," +
                "'path' = '/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score.csv'," +
                "'separator' = ','," +
                "'parallelism' = '2'" +
                ")";

        tableEnv.executeSql(odsScoreDdl);

//        tableEnv.createTemporaryFunction("func1", Function1.class);

        tableEnv.executeSql("select stu_no, sub_no, score from ods_score").print();
    }

    public static class Function1 extends ScalarFunction {

        @Override
        public void open(FunctionContext context) throws Exception {
       }

        @Override
        public void close() throws Exception {
        }

        /**
         * 当num为0，并且当前时间的秒数为奇数时，将会抛出除数为0的异常
         * @param num
         * @return
         */
        public int eval(int num) {
            if (num == 0 && LocalDateTime.now().getSecond() % 2 == 0) {
                num = 10;
            }
            return 10 / num;
        }
    }
}
