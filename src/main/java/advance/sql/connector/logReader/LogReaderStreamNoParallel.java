package advance.sql.connector.logReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.*;
import java.nio.file.*;
import java.util.List;

/**
 * 继承RichSourceFunction，实现具体读取数据的逻辑
 *
 * 包含两个阶段：读取快照阶段、读取增量数据阶段
 * 读取快照阶段是一次性的，在启动的时候执行
 * 读取增量数据阶段是持续性的，在读取快照完成后开始
 * 在读取快照时要注意此时若源数据发生了变更的话，需要考虑在快照完成后马上去读取这些增量数据。也可以考虑对源数据加写入锁，防止读取快照时发生数据变更
 * 在此连接器中由于读取快照时，若发生数据变更，也能读取到新增的行，因此不需要再考虑这个问题
 */
@Slf4j
public class LogReaderStreamNoParallel extends RichSourceFunction<RowData> implements CheckpointedFunction {
    private final String path;
    private final DeserializeFormatter deserializeFormatter;
    private boolean running = true;
    private int position = 0;
    private ListState<Integer> state;


    public LogReaderStreamNoParallel(String path, String separator,
                                     List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.path = path;
        this.deserializeFormatter = new DeserializeFormatter(columnSchema, separator);
    }

    /**
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("source open");
    }

    /**
     * 通过重写run方法，来实现读取逻辑，并通过SourceContext把数据发到flinksql表中，每条数据的类型要求为RowData
     */
    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        // 读取快照
        readSnapshot(ctx);

        // 读取增量数据

        Object lock = ctx.getCheckpointLock();

        Path path = Paths.get(this.path);
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            path.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (running) {
                // 阻塞当前线程，直至监听到文件变更
                WatchKey key = watchService.take();
                try (LineIterator lineIterator = FileUtils.lineIterator(new File(this.path), "UTF-8")) {
                    int rowNum = 0;
                    // 一旦监听到数据发生变更，则从position位置往后读取所有行。在此期间，若数据又发生变更，新行也会被读取到
                    while (lineIterator.hasNext()) {
                        String line = lineIterator.nextLine();
                        // 每次都需要从第一行开始扫描，扫描到position记录的位置后才开始处理行数据
                        if (rowNum < position) {
                            rowNum++;
                            continue;
                        }
                        rowNum++;
                        synchronized (lock) {
                            position++;
                            ctx.collect(deserializeFormatter.deserializeToRowData(line));
                        }
                    }
                } catch (IOException e) {
                    log.error("read file error, {}", e.getMessage());
                }
                key.reset();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        this.running = false;
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    /**
     * 读取快照
     * @param ctx
     * @throws Exception
     */
    private void readSnapshot(SourceContext<RowData> ctx) throws Exception {
        int rowNum = 0;
        try (LineIterator lineIterator = FileUtils.lineIterator(new File(this.path), "UTF-8")) {
            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                if (rowNum < position) {
                    rowNum++;
                    continue;
                }
                rowNum++;
                position++;
                ctx.collect(deserializeFormatter.deserializeToRowData(line));
            }
            updateState(position);
        } catch (IOException e) {
            log.error("read file error, {}", e.getMessage());
        }
    }

    /**
     * 在进行checkpoint的时候调用
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        updateState(position);
        System.out.println("update state to " + position);
    }

    /**
     * 在open前调用一次，初始化所需的状态
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<Integer>("position", int.class)
        );

        position = getState();
        System.out.println("init state as " + position);
    }

    private int getState () throws Exception {
        int pos = 0;
        for (int s: state.get()) {
            pos = s;
        }
        return pos;
    }

    private void updateState (int s) throws Exception {
        state.clear();
        state.add(s);
    }
}
