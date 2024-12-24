package advance.sql.connector.logReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;

/**
 * 继承RichInputFormat，实现具体读取数据的逻辑
 * 通过算子并行度参数parallelism和分区index两个参数来实现并行读取
 * 当源文件的行号等于parallelism与index的模时，表示该行数据归该子任务处理
 */
@Slf4j
public class LogReaderStreamParallel extends RichInputFormat<RowData, InputSplit> {
    private final int parallelism;
    private int index;
    private final String path;
    private LineIterator lineIterator;
    private int position = -1;
    private final DeserializeFormatter deserializeFormatter;
    private WatchService watchService;
    private boolean snapshotFinished = false;
    private boolean rowRemain = false;


    public LogReaderStreamParallel(String path, String separator,
                                   List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema,
                                   int parallelism) {
        this.path = path;
        this.deserializeFormatter = new DeserializeFormatter(columnSchema, separator);
        this.parallelism = parallelism;
    }


    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    /**
     * 初始化所有分区
     *
     * @param minNumSplits Number of minimal input splits, as a hint.
     * @return
     * @throws IOException
     */
    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        GenericInputSplit[] ret = new GenericInputSplit[parallelism];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    /**
     * 在open方法里获取并行度和当前子任务的index
     *
     * @param split The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(InputSplit split) throws IOException {
        index = split.getSplitNumber();
        System.out.println("source open @ " + index);

        File file = new File(path);
        this.lineIterator = FileUtils.lineIterator(file, "UTF-8");

        Path path = Paths.get(this.path);
        watchService = FileSystems.getDefault().newWatchService();
        path.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
    }

    /**
     * 声明读取是否已经结束
     *
     * @return
     * @throws IOException
     */
    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    /**
     * 通过重写nextRecord方法，来实现读取逻辑，每次返回读取的下一条数据
     */
    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!snapshotFinished) {
            return readLineFromSnapshot();
        } else if (rowRemain) {
            return readLineFromRemain();
        } else {
            return readLineRealtime();
        }
    }

    @Override
    public void close() throws IOException {
        lineIterator.close();
    }

    /**
     * 返回一行快照数据
     * @return
     * @throws IOException
     */
    private RowData readLineFromSnapshot() throws IOException {
        if (!lineIterator.hasNext()) {
            snapshotFinished = true;
            lineIterator.close();
            return readLineRealtime();
        }
        String line = lineIterator.nextLine();
        position++;
        if (position % parallelism != index) {
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
            WatchKey key = watchService.take();
            key.pollEvents();
            key.reset();
            lineIterator = FileUtils.lineIterator(new File(this.path), "UTF-8");
            int rowNum = -1;
            String line;
            do {
                line = lineIterator.nextLine();
                rowNum++;
            } while (rowNum <= position);
            position++;
            if (position % parallelism != index) {
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
        } catch (InterruptedException | IOException e) {
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
        position++;
        if (position % parallelism != index) {
            if (!rowRemain) {
                return readLineRealtime();
            } else {
                return readLineFromRemain();
            }
        }
        return deserializeFormatter.deserializeToRowData(line);
    }
}
