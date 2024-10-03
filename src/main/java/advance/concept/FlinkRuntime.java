package advance.concept;

import basic.sql.util.TableUtil;
import conf.MysqlConf;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;


/**
 * flink代码运行浅析
 * 1. flink架构上包含了job manager进程和task manager进程，job manager负责管理flink任务和task manager，job manager只有一个；
 * task manager负责执行flink任务，task manager至少有一个
 * 2. 除了job manager和task manager外，flink还有一个client进程，该进程不是运行时常驻，它负责提交解析过的代码给job manager，之后可以退出。
 * 3. flink任务具体是通过flink算子实现的，一个flink算子就是一个具体的操作，通过把多个flink算子串起来实现计算任务
 * 4. flink算子是分布式的，它至少有1个线程来执行，也就是说并行度至少声明为1
 * 5. flink算子本质是是一个类，它需要继承自flink提供的父类。flink算子类中部分方法在client上执行（包含构造方法等），另外一部分方法则在task manager上执行，
 * 在task manager上执行的那些方法具有并行度（包含open方法、close方法、具体执行计算的方法等）
 * 6. flink算子类的类成员必须是可序列化的，除非注解为Transient不参与序列化。在client接收到代码后，会把算子类创建出实例，因此，算子类的实例化
 * 是在client上进行的。而之后client会把创建出来的算子实例进行序列化，再发送给job manager，再分别发给各个task manager。task manager接收到序列化文件后
 * 会进行反序列化，又得到了算子实例，并且后续执行算子实例的open、计算、close等方法
 * 7. 在提交的代码中，main方法运行在client端。在某些场景下，client端跟job manager绑定在一起，main方法会运行在job manager进程里。(application mode)
 */

@Slf4j
public class FlinkRuntime {
    public static void main(String[] args) {
        log.info("running main method");

        TableEnvironment tableEnv = TableUtil.getStreamTableEnv(2);

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
                "'format' = 'json'" +
                ")";

        String dwdScoreDdl = "create table dwd_score(" +
                "stu_no int," +
                "stu_name string," +
                "sub_no int," +
                "score int" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'dwd_score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'value.format' = 'json'" +
                ")";

        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql(dwdScoreDdl);

        tableEnv.createFunction("get_stu_name", GetStudentName.class);

        tableEnv.executeSql("insert into dwd_score(stu_no, stu_name, sub_no, score) " +
                "select stu_no, get_stu_name(stu_no) as stu_name, sub_no, score " +
                "from ods_score");

        log.info("finish");

    }

    /**
     * udf也是一种算子，跟flink常用的map、filter等方法都是一个道理的
     */
    @Slf4j
    public static class GetStudentName extends ScalarFunction {
        private Map<Integer, String> data;

        /**
         * 算子构造器方法，执行在client上
         * 执行时机为提交代码后
         */
        public GetStudentName() {
            log.info("running construct method");
        }

        /**
         * 算子open方法，执行在task manager上
         * 执行时机为接收到job manager的序列化文件并进行反序列化后
         */
        @Override
        public void open(FunctionContext context) throws Exception {
            log.info("running open method");
            this.data = new HashMap<>();
            try (Connection connection = DriverManager.getConnection(MysqlConf.URL, MysqlConf.USERNAME, MysqlConf.PASSWORD)){
                try (PreparedStatement preparedStatement =
                             connection.prepareStatement("select stu_no, stu_name from dim_student")){
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            int stuNo = resultSet.getInt(1);
                            String stuName = resultSet.getString(2);
                            data.put(stuNo, stuName);
                        }
                    }
                }
            }
        }

        /**
         * 算子close方法，执行在task manager上
         * 执行时机为算子终结前，通常为任务结束或异常退出后
         */
        @Override
        public void close() throws Exception {
            log.info("running close method");
        }

        /**
         * 算子计算方法，执行在task manager上
         * 执行时机为接收到数据时
         */
        public String eval(Integer stuNo) {
            return data.getOrDefault(stuNo, null);
        }
    }
}
