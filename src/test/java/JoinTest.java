import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class JoinTest {
    @Data
    @ToString
    static class Data1{
        private int id;
        private String name;
    }

    @Data
    @ToString
    static class Data2{
        private int id;
        private int score;
    }

    static class MapData1 extends RichMapFunction<String, Data1>{
        private final ObjectMapper objectMapper;

        public MapData1(){
            this.objectMapper = new ObjectMapper();
        }
        @Override
        public Data1 map(String s) throws Exception {
            return objectMapper.readValue(s, Data1.class);
        }
    }

    static class MapData2 extends RichMapFunction<String, Data2>{
        private final ObjectMapper objectMapper;

        public MapData2(){
            this.objectMapper = new ObjectMapper();
        }
        @Override
        public Data2 map(String s) throws Exception {
            return objectMapper.readValue(s, Data2.class);
        }
    }

    static class Transformer extends KeyedCoProcessFunction<Integer, Data1, Data2, String>{
        private transient ValueState<Data1> data1Cache;
        private transient ValueState<Data2> data2Cache;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Data1> descriptor1 = new ValueStateDescriptor<>("data1", TypeInformation.of(Data1.class), null);
            ValueStateDescriptor<Data2> descriptor2 = new ValueStateDescriptor<>("data2", TypeInformation.of(Data2.class), null);
            data1Cache = getRuntimeContext().getState(descriptor1);
            data2Cache = getRuntimeContext().getState(descriptor2);
        }

        @Override
        public void processElement1(Data1 data1, KeyedCoProcessFunction<Integer, Data1, Data2, String>.Context context, Collector<String> collector) throws Exception {
            System.out.printf("data1 id %d 来了%n", data1.getId());
            if (data2Cache.value() == null){
                System.out.println("未匹配到data2, 先缓存data1");
                data1Cache.update(data1);
            } else {
                System.out.println("匹配到data2, 一并输出");
                collector.collect(data1.toString() + data2Cache.value().toString());
            }
        }

        @Override
        public void processElement2(Data2 data2, KeyedCoProcessFunction<Integer, Data1, Data2, String>.Context context, Collector<String> collector) throws Exception {
            System.out.printf("data2 id %d 来了%n", data2.getId());
            if (data1Cache.value() == null){
                System.out.println("未匹配到data1, 先缓存data2");
                data2Cache.update(data2);
            } else {
                System.out.println("匹配到data1, 一并输出");
                collector.collect(data1Cache.value().toString() + data2.toString());
            }
        }


    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource1 = KafkaSource.<String>builder()
                .setBootstrapServers("10.150.9.36:9092")
                .setTopics("jolin")
                .setGroupId("data-1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> kafkaSource2 = KafkaSource.<String>builder()
                .setBootstrapServers("10.150.9.36:9092")
                .setTopics("jolin2")
                .setGroupId("data-2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<Data1> data1 = streamExecutionEnvironment
                .fromSource(kafkaSource1, WatermarkStrategy.noWatermarks(),"data1")
                .map(new MapData1());

        SingleOutputStreamOperator<Data2> data2 = streamExecutionEnvironment
                .fromSource(kafkaSource2, WatermarkStrategy.noWatermarks(),"data2")
                .map(new MapData2());

        data1.connect(data2)
                .keyBy((KeySelector<Data1, Integer>) Data1::getId, (KeySelector<Data2, Integer>) Data2::getId)
                .process(new Transformer())
                .print();

        streamExecutionEnvironment.execute();
    }
}
