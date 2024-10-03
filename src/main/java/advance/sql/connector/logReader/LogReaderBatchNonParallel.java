package advance.sql.connector.logReader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.File;
import java.util.List;

/**
 * 继承RichSourceFunction，实现具体读取数据的逻辑
 * 只能单并行度读取
 */
public class LogReaderBatchNonParallel extends RichSourceFunction<RowData> {
    private final String path;
    private LineIterator lineIterator;
    private final DeserializeFormatter deserializeFormatter;
    private boolean running = true;

    public LogReaderBatchNonParallel (String path, String separator,
                                      List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.path = path;
        this.deserializeFormatter = new DeserializeFormatter(columnSchema, separator);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        File file = new File(path);
        this.lineIterator = FileUtils.lineIterator(file, "UTF-8");
    }

    /**
     * 读取数据的主方法
     * @param ctx The context to emit elements to and for accessing locks.
     * @throws Exception
     */
    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (running && lineIterator.hasNext()) {
            String line = lineIterator.nextLine();
            ctx.collect(deserializeFormatter.deserializeToRowData(line));
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public void close() throws Exception {
        this.running = false;
    }
}
