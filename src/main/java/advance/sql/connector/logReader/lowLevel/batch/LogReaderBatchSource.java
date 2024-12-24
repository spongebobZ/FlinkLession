package advance.sql.connector.logReader.lowLevel.batch;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;

/**
 * RowData: 提取到下游的数据类型
 * LogSplit: 读取的数据分片类型
 * LogCheckpoint: 持久化还没有分配的splits
 */
public class LogReaderBatchSource implements Source<RowData, LogSplit, LogCheckpoint> {
    private final String filePath;
    private final String separator;
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;
    public LogReaderBatchSource(String filePath, String separator,
                                List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.filePath = filePath;
        this.separator = separator;
        this.columnSchema = columnSchema;
    }

    /**
     * 声明读取方式是批读取
     * @return
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /**
     * 创建SplitEnumerator
     * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
     * @return
     * @throws Exception
     */
    @Override
    public SplitEnumerator<LogSplit, LogCheckpoint> createEnumerator(SplitEnumeratorContext<LogSplit> enumContext) throws Exception {
        return new LogEnumerator(enumContext, filePath);
    }

    /**
     * 从checkpoint恢复SplitEnumerator
     * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
     *     enumerator.
     * @param checkpoint The checkpoint to restore the SplitEnumerator from.
     * @return
     * @throws Exception
     */
    @Override
    public SplitEnumerator<LogSplit, LogCheckpoint> restoreEnumerator(SplitEnumeratorContext<LogSplit> enumContext, LogCheckpoint checkpoint) throws Exception {
        return new LogEnumerator(enumContext, filePath, checkpoint.getSplits());
    }

    @Override
    public SimpleVersionedSerializer<LogSplit> getSplitSerializer() {
        return new LogSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LogCheckpoint> getEnumeratorCheckpointSerializer() {
        return new LogCheckpointSerializer();
    }

    @Override
    public SourceReader<RowData, LogSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new LogSourceReader(readerContext, filePath, separator, columnSchema);
    }
}
