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
import java.util.List;

/**
 * 继承RichInputFormat，实现具体读取数据的逻辑
 * 通过算子并行度参数parallelism和分区index两个参数来实现并行读取
 * 当源文件的行号等于parallelism与index的模时，表示该行数据归该子任务处理
 */
@Slf4j
public class LogReaderBatchParallel extends RichInputFormat<RowData, InputSplit> {
    private final int parallelism;
    private int index;
    private final String path;
    private LineIterator lineIterator;
    private int position = -1;
    private final DeserializeFormatter deserializeFormatter;


    public LogReaderBatchParallel(String path, String separator,
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
    }

    /**
     * 声明读取是否已经结束
     * @return
     * @throws IOException
     */
    @Override
    public boolean reachedEnd() throws IOException {
        return !lineIterator.hasNext();
    }

    /**
     * 通过重写nextRecord方法，来实现读取逻辑，每次返回读取的下一条数据
     * 框架会循环调用此方法, 返回值为null时将停止读取并退出
     */
    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!lineIterator.hasNext()) {
            return null;
        }
        String line = lineIterator.nextLine();
        position++;
        if (position % parallelism != index) {
            return nextRecord(reuse);
        }
        return deserializeFormatter.deserializeToRowData(line);
    }

    @Override
    public void close() throws IOException {
        lineIterator.close();
    }
}
