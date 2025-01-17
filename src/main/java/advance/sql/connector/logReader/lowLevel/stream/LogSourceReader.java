package advance.sql.connector.logReader.lowLevel.stream;

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
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogSourceReader implements SourceReader<RowData, LogSplit> {
    private final SourceReaderContext readerContext;
    private LogSplit split;
    private final AtomicBoolean running;
    private CompletableFuture<Void> splitsAvailable;

    private int parallelism;
    private int index;
    private final String path;
    private LineIterator lineIterator;
    private final DeserializeFormatter deserializeFormatter;
    private WatchService watchService;
    private boolean rowRemain = false;
    private final AtomicBoolean checkedAvailable = new AtomicBoolean(false);



    public LogSourceReader(SourceReaderContext readerContext, String path, String separator, List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.readerContext = readerContext;
        this.running = new AtomicBoolean(false); // 初始化为未运行
        this.splitsAvailable = new CompletableFuture<>();

        this.path = path;
        this.deserializeFormatter = new DeserializeFormatter(columnSchema, separator);
    }

    @Override
    public void start() {
        File file = new File(path);
        try {
            this.lineIterator = FileUtils.lineIterator(file, "UTF-8");
            Path path = Paths.get(this.path);
            watchService = FileSystems.getDefault().newWatchService();
            path.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.running.set(true); // 标识为运行中
        readerContext.sendSplitRequest(); // 获取split
    }

    /**
     * 拉取n条数据，返回当前这一次的数据poll结果
     * - 如果分片中有数据可读，则返回 {@code InputStatus.MORE_AVAILABLE}，底层会继续调用pollNext方法
     * - 如果当前没有数据但未来可能有数据，则返回 {@code InputStatus.NOTHING_AVAILABLE}，底层将暂停调用pollNext方法
     * - 如果所有分片的数据都读取完毕，则返回 {@code InputStatus.END_OF_INPUT}，底层将退出读取
     */
    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (!running.get()){
            return InputStatus.END_OF_INPUT;
        }
        if (split == null) {
            // 此时split未申请到（addSplits方法必然也未执行，所以splitsAvailable状态也未完成），
            // 返回当前split无数据，则底层会暂停调用pollNext方法，直至splitsAvailable变量状态置为完成
            return InputStatus.NOTHING_AVAILABLE;
        }
        RowData rowData = readOneRow();
        if (rowData == null) {
            this.splitsAvailable = new CompletableFuture<>();
            checkSplitAvailable();
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(rowData);
        // 因为是流读取持续性消费，返回后续有数据可读
        return InputStatus.MORE_AVAILABLE;
    }

    /**
     * flink会周期性调用此方法来checkpointing方法返回的数据
     * @param checkpointId
     * @return
     */
    @Override
    public List<LogSplit> snapshotState(long checkpointId) {
        return Collections.singletonList(this.split);
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

    private void checkSplitAvailable() {
        CompletableFuture.runAsync(() -> {
            try {
                WatchKey watchKey = this.watchService.take();
                watchKey.pollEvents();
                watchKey.reset();
                this.checkedAvailable.set(true);
                this.splitsAvailable.complete(null);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * LogEnumerator会分发split到各个source reader
     * @param splits The splits assigned by the split enumerator.
     */
    @Override
    public void addSplits(List<LogSplit> splits) {
        LogSplit split = splits.get(0);
        this.split = split;
        this.parallelism = split.getWorkerCount();
        this.index = split.getCurrentWorkerId();
        this.splitsAvailable.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() throws Exception {
        this.running.set(false);
    }

    private RowData readOneRow() throws IOException {
        if (!split.isSnapshotFinished()) {
            return readLineFromSnapshot();
        } else if (rowRemain) {
            return readLineFromRemain();
        } else {
            return readLineRealtime();
        }
    }

    /**
     * 返回一行快照数据
     * @return
     * @throws IOException
     */
    private RowData readLineFromSnapshot() throws IOException {
        if (!lineIterator.hasNext()) {
            split.setSnapshotFinished();
            lineIterator.close();
            return readLineRealtime();
        }
        String line = lineIterator.nextLine();
        split.increaseCurrentRowNum();
        if (split.getCurrentRowNum() % parallelism != index) {
            return readLineFromSnapshot();
        }
        return deserializeFormatter.deserializeToRowData(line);
    }

    /**
     * 返回一行实时新增的数据
     * @return
     */
    private RowData readLineRealtime() {
        try {
            if (!checkedAvailable.get()) {
                WatchKey key = watchService.poll(); // 使用非阻塞的poll
                if (key == null) {
                    return null;
                }
                key.pollEvents();
                key.reset();
            } else {
                checkedAvailable.set(false);
            }
            lineIterator = FileUtils.lineIterator(new File(this.path), "UTF-8");
            int rowNum = 0;
            String line;
            do {
                line = lineIterator.nextLine();
                rowNum++;
            } while (rowNum <= split.getCurrentRowNum());
            split.increaseCurrentRowNum();
            if (split.getCurrentRowNum() % parallelism != index) {
                if (lineIterator.hasNext()) {
                    return readLineFromRemain();
                } else {
                    return readLineRealtime();
                }
            }
            if (lineIterator.hasNext()) {
                rowRemain = true;
            }
            return deserializeFormatter.deserializeToRowData(line);
        } catch (IOException | NoSuchElementException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 一次性新增多条数据时，第一条数据由readLineRealtime读取，而后由此方法读取剩余的数据
     * @return
     * @throws IOException
     */
    private RowData readLineFromRemain() throws IOException {
        String line = lineIterator.nextLine();
        if (!lineIterator.hasNext()) {
            rowRemain = false;
            lineIterator.close();
        }
        split.increaseCurrentRowNum();
        if (split.getCurrentRowNum() % parallelism != index) {
            if (!rowRemain) {
                return readLineRealtime();
            } else {
                return readLineFromRemain();
            }
        }
        return deserializeFormatter.deserializeToRowData(line);
    }
}
