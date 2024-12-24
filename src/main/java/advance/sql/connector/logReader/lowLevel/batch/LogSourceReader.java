package advance.sql.connector.logReader.lowLevel.batch;

import advance.sql.connector.logReader.DeserializeFormatter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

public class LogSourceReader implements SourceReader<RowData, LogSplit> {
    private final SourceReaderContext readerContext;
    private Queue<LogSplit> splits;
    private boolean running;
    private CompletableFuture<Void> splitsAvailable;
    private final String path;
    private final DeserializeFormatter deserializeFormatter;



    public LogSourceReader(SourceReaderContext readerContext, String path, String separator, List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.readerContext = readerContext;
        this.splitsAvailable = new CompletableFuture<>();

        this.path = path;
        this.deserializeFormatter = new DeserializeFormatter(columnSchema, separator);
    }

    @Override
    public void start() {
        this.splits = new ArrayBlockingQueue<>(10000);
        this.running = true; // 标识为运行中
    }

    /**
     * 拉取n条数据，返回当前这一次的数据poll结果
     * - 如果分片中有数据可读，则返回 {@code InputStatus.MORE_AVAILABLE}，底层会继续调用pollNext方法
     * - 如果当前没有数据但未来可能有数据，则返回 {@code InputStatus.NOTHING_AVAILABLE}，底层将暂停调用pollNext方法
     * - 如果所有分片的数据都读取完毕，则返回 {@code InputStatus.END_OF_INPUT}，底层将退出读取
     */
    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (!running){
            return InputStatus.END_OF_INPUT;
        }
        // 设定为每次消费一个split
        LogSplit logSplit = splits.peek();
        if (logSplit == null) {
            // 队列里无split，向enumerator申请新的split
            readerContext.sendSplitRequest();
            // 返回当前split无数据，则底层会暂停调用pollNext方法，直至splitsAvailable变量状态置为完成
            this.splitsAvailable = new CompletableFuture<>();
            return InputStatus.NOTHING_AVAILABLE;
        }
        for (RowData rowData: readSplit(logSplit)) {
            output.collect(rowData);
        }
        // split的数据消费完毕后，再从队列里移除split
        splits.remove();
        // 返回有数据可读，继续消费下一个split
        return InputStatus.MORE_AVAILABLE;
    }

    /**
     * flink会周期性调用此方法来checkpointing返回的数据
     * @param checkpointId
     * @return
     */
    @Override
    public List<LogSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(this.splits);
    }

    /**
     * 1. 初次启动时，不管isAvailable的future状态是否已完成，都会持续性调用pollNext方法获取数据
     * 2. 当InputStatus返回的是一个NOTHING_AVAILABLE状态时，需要把该future会变为not completed，然后底层会再次调用isAvailable方法
     * 来等待该future变为completed
     * 3. 当isAvailable的future completed时，底层会持续性的调用pollNext方法读取数据
     * @return
     */
    @Override
    public CompletableFuture<Void> isAvailable() {
        return splitsAvailable;
    }

    /**
     * LogEnumerator会分发split到各个source reader
     * source reader接收到splits后，通知底层split已就绪，底层将恢复调用pollNext方法读取数据
     * @param splits The splits assigned by the split enumerator.
     */
    @Override
    public void addSplits(List<LogSplit> splits) {
        this.splits.addAll(splits);
        this.splitsAvailable.complete(null);
    }

    /**
     * enumerator通知source reader无split可申请后的回调
     */
    @Override
    public void notifyNoMoreSplits() {
        System.out.println("no more splits");
        this.running = false;
        this.splitsAvailable.complete(null);
    }

    @Override
    public void close() throws Exception {
        this.running = false;
        System.out.println("source reader closed");
    }

    private List<RowData> readSplit(LogSplit split) throws IOException {
        List<RowData> rows = new ArrayList<>(split.getHighRowNum() - split.getLowRowNum() + 1);
        File file = new File(path);
        try(LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8")){
            int rowNum = 0;
            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                rowNum++;
                if (rowNum < split.getLowRowNum()) {
                    continue;
                }
                if (rowNum > split.getHighRowNum()) {
                    break;
                }
                rows.add(deserializeFormatter.deserializeToRowData(line));
            }
        }
        return rows;
    }
}
