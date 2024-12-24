package advance.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;



public class FraudDetection {

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟输入流：交易事件
        DataStream<Transaction> transactions = env.fromElements(
                new Transaction("user1", 1620132000L, 12000),
                new Transaction("user1", 1620132060L, 15000),
                new Transaction("user1", 1620132120L, 13000),
                new Transaction("user2", 1620133000L, 5000)
        ).keyBy((KeySelector<Transaction, String>) value -> value.userId)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                .withTimestampAssigner((t, ts) -> t.timestamp));

        // 定义 CEP 模式：用户在 1 分钟内进行 3 次大额交易
        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("trade")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.amount > 10000;  // 检测大额交易
                    }
                }).oneOrMore()
                .within(Time.minutes(1));  // 事件必须在 1 分钟内发生

        // 将模式应用于流数据, 满足pattern的序列将会进入select方法，以下pattern1就是事件序列
        DataStream<String> result = CEP.pattern(transactions, pattern)
                .select((PatternSelectFunction<Transaction, String>) pattern1 -> {
                    // 当模式匹配时，输出用户ID和匹配到的交易信息
                    Transaction trade = pattern1.get("trade").get(0);
                    System.out.println(pattern1.get("trade").size());
                    return String.format("用户%s交易额达到%f!", trade.getUserId(), trade.getAmount());
                });

        // 打印结果
        result.print();

        // 执行 Flink 程序
        env.execute("Flink CEP Fraud Detection");
    }
}

@Data
@AllArgsConstructor
class Transaction {
    public String userId;
    public long timestamp; // 事件时间
    public double amount;  // 交易金额

    // 构造方法、getters 和 setters
}


