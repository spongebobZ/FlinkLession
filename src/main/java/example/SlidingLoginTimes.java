package example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 统计过去7天内，连续登录3天以上的用户，要求10分钟更新一次
 * <p>
 * 类似过去一段的统计，都可以使用滑动窗口来统计，在使用滑动窗口统计时，需要明确的是滑动频率，相对于业务来说就是多久更新一次结果，如果更新频率过高，
 * 可能会影响性能，需要优化滑动窗口，考虑预聚合小窗口
 */
public class SlidingLoginTimes {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("login_records")
                .setGroupId("study")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // kafka消息格式: {"user_id": 1, "login_time": "2025-09-29 09:00:00"}
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");


        kafkaStream
                .map(new RichMapFunction<String, LoginRecord>() {
                    final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public LoginRecord map(String value) throws Exception {
                        return objectMapper.readValue(value, LoginRecord.class);
                    }
                })
                .keyBy(LoginRecord::getUserId)
                .window(SlidingProcessingTimeWindows.of(Duration.ofDays(7), Duration.ofMinutes(10)))
                .process(new CountLoginTimes())
                .print();

        env.execute();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    private static class LoginRecord {
        @JsonProperty("user_id")
        private Integer userId;
        @JsonProperty("login_time")
        private String loginTime;
    }

    /**
     * 统计7天窗口内的用户登录数据，若登录次数>=3，则输出用户ID、统计的时间，后面业务按照该时间去查询即可
     */
    private static class CountLoginTimes extends ProcessWindowFunction<LoginRecord, String, Integer, TimeWindow> {
        @Override
        public void process(Integer userId, ProcessWindowFunction<LoginRecord, String, Integer, TimeWindow>.Context context, Iterable<LoginRecord> elements, Collector<String> out) throws Exception {
            Set<String> distinctLoginDays = new HashSet<>();
            for (LoginRecord element : elements) {
                if (element.loginTime == null) {
                    continue;
                }
                distinctLoginDays.add(element.getLoginTime().substring(0, 10));
            }
            if (distinctLoginDays.size() < 3) {
                // 登录天数不足3次，不输出
                return;
            }
            String[] sortedLoginDays = distinctLoginDays.toArray(new String[0]);
            Arrays.sort(sortedLoginDays);
            int consecutiveCount = 1;
            for (int i = 1; i < sortedLoginDays.length; i++) {
                String curDay = sortedLoginDays[i];
                String prevDay = sortedLoginDays[i - 1];
                if (isConsecutiveDay(prevDay, curDay)) {
                    consecutiveCount++;
                } else {
                    consecutiveCount = 1;
                }
                if (consecutiveCount >= 3) {
                    long windowEnd = context.window().getEnd();
                    out.collect(userId + "," + windowEnd);
                    break;
                }
            }
        }
    }

    /**
     * 判断两个日期是否是连续的
     */
    private static boolean isConsecutiveDay(String date1, String date2) {
        try {
            LocalDate d1 = LocalDate.parse(date1);
            LocalDate d2 = LocalDate.parse(date2);
            return d2.minusDays(1).equals(d1);
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
