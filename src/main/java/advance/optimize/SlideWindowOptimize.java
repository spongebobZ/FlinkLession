package advance.optimize;

import lombok.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 优化滑动窗口
 * <p>
 * 背景：滑动窗口长，滑动频率高，导致计算效率低
 * <p>
 * 思路：按滑动频率先划分成多个预聚合小窗口，再对预聚合小窗口进行滑动窗口统计，大幅减少小窗口数据的重复计算
 */
public class SlideWindowOptimize {
    /**
     * 需求：统计每个事件60分钟内出现的次数，需要每分钟更新一次结果
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 用单并行度测试窗口，避免测试数据不足导致窗口水位线不移动，无法触发计算
        env.setParallelism(1);

        env
                // 创建kafka数据流
                .fromSource(getKafkaSource(), WatermarkStrategy.noWatermarks(), "Kafka Source")
                // 处理成Event对象
                .map(new MapToEventFunction())
                // 声明event time, 允许5秒的延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp()))
                .keyBy(Event::getEvent)
                // 按滑动频率划分预聚合小窗口
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new AggEventsInMinute())
                .keyBy(EventAgg::getEvent)
                // 开启滑动窗口，对分钟级别的预聚合数据进行滑动统计
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(60), Duration.ofMinutes(1)))
                .process(new AggEventsInHour())
                .print();

        env.execute("SlideWindowOptimize");
    }

    private static KafkaSource<String> getKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    @Getter
    @AllArgsConstructor
    private static class Event {
        private String event;
        private Long timestamp;
    }


    /**
     * kafka数据流的格式: event,timestamp
     */
    private static class MapToEventFunction implements MapFunction<String, Event> {
        @Override
        public Event map(String value) throws Exception {
            String[] fields = value.split(",");
            return new Event(fields[0], Long.valueOf(fields[1]));
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    private static class EventAgg {
        private String event;
        private Long timestamp;
        private Long count = 0L;

        public void increaseCount() {
            count++;
        }

        public void merge(EventAgg other) {
            this.count += other.getCount();
        }
    }

    /**
     * 聚合函数，对同一个事件的数据进行计数统计
     */
    private static class AggEventsInMinute implements AggregateFunction<Event, EventAgg, EventAgg> {
        @Override
        public EventAgg createAccumulator() {
            return new EventAgg();
        }

        @Override
        public EventAgg add(Event event, EventAgg eventAgg) {
            eventAgg.setEvent(event.getEvent());
            eventAgg.setTimestamp(event.getTimestamp());
            eventAgg.increaseCount();
            return eventAgg;
        }

        @Override
        public EventAgg getResult(EventAgg eventAgg) {
            return eventAgg;
        }

        @Override
        public EventAgg merge(EventAgg eventAgg, EventAgg acc1) {
            eventAgg.merge(acc1);
            return eventAgg;
        }
    }

    /**
     * 窗口函数，对分钟级别的数据进行滑动窗口统计
     */
    private static class AggEventsInHour extends ProcessWindowFunction<EventAgg, EventAgg, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<EventAgg, EventAgg, String, TimeWindow>.Context context, Iterable<EventAgg> elements, Collector<EventAgg> out) throws Exception {
            EventAgg result = new EventAgg();
            for (EventAgg element : elements) {
                result.merge(element);
            }
            result.setTimestamp(context.window().getEnd());
            out.collect(result);
        }
    }
}


