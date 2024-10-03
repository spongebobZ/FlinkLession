package advance.sql.connector.logReader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * 实现ScanTableSource接口，而ScanTableSource接口是DynamicTableSource的子类
 * 用于声明表运行时信息
 */
public class LogReaderSource implements ScanTableSource {
    private final String mode;
    private final String path;
    private final String separator;
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;
    private final int parallelism;
    public LogReaderSource(String mode, String path, String separator,
                           List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema, int parallelism) {
        this.mode = mode;
        this.path = path;
        this.separator = separator;
        this.columnSchema = columnSchema;
        this.parallelism = parallelism;
    }

    /**
     * 复制该对象的方法，在运行时存在复制到多个线程里去的需求
     */
    @Override
    public DynamicTableSource copy() {
        return new LogReaderSource(mode, path, separator, columnSchema, parallelism);
    }

    /**
     * 连接器描述
     */
    @Override
    public String asSummaryString() {
        return "log reader";
    }

    /**
     * 连接器输入数据的op类型
     */
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    /**
     * 重写getScanRuntimeProvider方法，返回ScanRuntimeProvider
     * ScanRuntimeProvider用于进行从数据源读取数据的行为
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (mode.equalsIgnoreCase("batch")) {
            return InputFormatProvider.of(new LogReaderBatchParallel(path, separator, columnSchema, parallelism));
        } else {
            return InputFormatProvider.of(new LogReaderStreamParallel(path, separator, columnSchema, parallelism));
        }
    }
}
