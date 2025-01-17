package advance.sql.connector.logWriter;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * 实现DynamicTableSink接口
 * 用于声明表运行时信息
 */
public class LogWriterSink implements DynamicTableSink {
    private final String mode;
    private final String path;
    private final String separator;
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;
    private final int parallelism;
    private final int batchSize;

    public LogWriterSink(String mode, String path, String separator,
                         List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema, int parallelism,
                         int batchSize) {
        this.mode = mode;
        this.path = path;
        this.separator = separator;
        this.columnSchema = columnSchema;
        this.parallelism = parallelism;
        this.batchSize = batchSize;
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // 考虑到为文件类型输出，因此只支持op为insert的数据输出；涉及到撤回流场景，可能需要添加op为update before、update after、delete的支持
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (mode.equalsIgnoreCase("batch")) {
            return SinkFunctionProvider.of(
                    new LogBatchWriteFunction(path, separator, columnSchema, batchSize), parallelism);
        } else {
            return SinkFunctionProvider.of(
                    new LogStreamWriteFunction(path, separator, columnSchema), parallelism);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new LogWriterSink(mode, path, separator, columnSchema, parallelism, batchSize);
    }

    /**
     * 连接器描述
     */
    @Override
    public String asSummaryString() {
        return "log writer";
    }
}
