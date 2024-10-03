package advance.concept;

import basic.sql.util.TableUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RestartTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        streamExecutionEnvironment.setParallelism(1);
        streamExecutionEnvironment.enableCheckpointing(1000L);
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
        configuration.set(PipelineOptions.OPERATOR_CHAINING, false);
        streamExecutionEnvironment.configure(configuration);

        streamExecutionEnvironment.fromSource(
                        KafkaSource.<String>builder()
                                .setBootstrapServers("localhost:9092")
                                .setTopics("ods_score")
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .setGroupId("test")
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build(),
                        WatermarkStrategy.noWatermarks(), "kafka")
                .map(new RichMapFunction<String, Integer>() {
                    private transient ValueState<Integer> count;
//                    private int count = 0;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("count",
                                TypeInformation.of(Integer.class), 0);
                        count = getRuntimeContext().getState(descriptor);
                        System.out.println("function1 open: " + getRuntimeContext().getIndexOfThisSubtask() + "," +
                                "count is " + count.value());
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("function1 close: " + getRuntimeContext().getIndexOfThisSubtask() + "," +
                                "count is " + count.value());
                    }

                    @Override
                    public Integer map(String value) throws Exception {
                        int v = Integer.parseInt(value);
                        System.out.println(count.value() + " + " + v);
                        count.update(count.value() + v);
                        return 10 - v;
                    }
                }).map(new RichMapFunction<Integer, Integer>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("function2 open: " + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("function2 close: "  + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return 10 / value;
                    }
                }).map(new RichMapFunction<Integer, Integer>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("function3 open: " + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("function3 close: "  + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return Math.abs(value);
                    }
                })
                .print();

        streamExecutionEnvironment.execute("test");
    }
}
