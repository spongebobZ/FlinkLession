package advance.concept;

import basic.sql.util.TableUtil;
import lombok.Data;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.LocalDateTime;

/**
 * 1. 如果未启用checkpoint，则默认无失败重启策略，算子收到异常时就会终止整个作业。若启用了checkpoint，默认使用重启策略
 * 2. 常用的失败重启策略有fixedDelayRestart、failureRateRestart、exponentialDelayRestart几种
 * 3. 默认重启方式是只重启失败子任务的上下游子任务，也可以设置所有算子的子任务一起重启
 * 4. udf要使用状态可以利用AggregateFunction(udaf聚合函数), udaf的ACC对象会保存到状态里, 前提是开启了checkpoint
 */
public class Restart {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableUtil.getFixedRestartStreamTableEnv(2, 3);

        tableEnv.createTemporaryFunction("func1", Function1.class);
        tableEnv.createTemporaryFunction("func2", Function2.class);

        String odsScoreDdl = "create table ods_score(" +
                "stu_no int," +
                "sub_no int," +
                "score int not null" +
                ") with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'ods_score'," +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
                ")";

        tableEnv.executeSql(odsScoreDdl);
        tableEnv.executeSql("select stu_no, func2(func1(score)) as num from ods_score group by stu_no").print();
    }

    public static class Function1 extends ScalarFunction {
        private int index;

        @Override
        public void open(FunctionContext context) throws Exception {
            Field streamingContext = context.getClass().getDeclaredField("context");
            streamingContext.setAccessible(true);
            StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) (streamingContext.get(context));
            index = streamingRuntimeContext.getIndexOfThisSubtask();
            System.out.println("function1 open @ " + index);
        }

        @Override
        public void close() throws Exception {
            System.out.println("function1 close @ " + index);
        }

        public int eval(int num) {
            if (num == 0 && LocalDateTime.now().getSecond() % 2 == 0) {
                num = 10;
            }
            return 10 / num;
        }
    }

    @Data
    public static class Acc implements Serializable {
        private int count = 0;

        public Acc() {

        }

        public void update(int num) {
            count += num;
        }


        public void reset() {
            count = 0;
        }
    }

    public static class Function2 extends AggregateFunction<Integer, Acc> {
        private int index;

        @Override
        public void open(FunctionContext context) throws Exception {
            Field streamingContext = context.getClass().getDeclaredField("context");
            streamingContext.setAccessible(true);
            StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) (streamingContext.get(context));
            index = streamingRuntimeContext.getIndexOfThisSubtask();
            System.out.println("function2 open @ " + index);
        }

        @Override
        public void close() throws Exception {
            System.out.println("function2 close @ " + index);
        }

        @Override
        public Integer getValue(Acc accumulator) {
            System.out.println("get acc " + accumulator.getCount());
            return accumulator.getCount();
        }

        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        public void resetAccumulator(Acc accumulator) {
            accumulator.reset();
        }

        public void accumulate(Acc accumulator, int num) {
            accumulator.update(num);
        }

        public void retract(Acc accumulator, int num) {
            System.out.println("acc retracted");
            accumulator.update(-num);
        }

        public void merge(Acc acc, Iterable<Acc> it) {
            for (Acc num : it) {
                acc.update(num.getCount());
            }
        }
    }
}
