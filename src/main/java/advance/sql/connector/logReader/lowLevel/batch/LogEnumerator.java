package advance.sql.connector.logReader.lowLevel.batch;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogEnumerator implements SplitEnumerator<LogSplit, LogCheckpoint> {
    private final SplitEnumeratorContext<LogSplit> context;
    private final String filePath;
    private List<LogSplit> splits;
    private final int splitSize = 3; // 定义每个split的行数上限

    /**
     * 用于新建LogEnumerator
     */
    public LogEnumerator(SplitEnumeratorContext<LogSplit> context, String filePath) {
        this.context = context;
        this.filePath = filePath;
    }

    /**
     * 用于从checkpoint数据中恢复LogEnumerator
     */
    public LogEnumerator(SplitEnumeratorContext<LogSplit> context, String filePath, List<LogSplit> splits){
        this.context = context;
        this.filePath = filePath;
        this.splits = splits;
    }



    /**
     * 生成所有分区的split
     */
    @Override
    public void start() {
        // 1. 获取总行数
        long lineCount = 0;
        try(BufferedReader reader = new BufferedReader(new FileReader(this.filePath))) {
            while (reader.readLine() != null) {
                lineCount++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 2. 划分split到splits数组里
        int splitCount = (int) (lineCount % splitSize == 0 ? lineCount / splitSize : lineCount / splitSize + 1);
        splits = new ArrayList<>(splitCount);
        LogSplit split;
        int lowRowNum = 1;
        int highRowNum = lowRowNum + splitSize;
        while (lowRowNum <= lineCount) {
            split = new LogSplit(filePath, lowRowNum, highRowNum);
            splits.add(split);
            lowRowNum = highRowNum + 1;
            highRowNum = lowRowNum + splitSize;
        }
    }

    /**
     * source reader会向enumerator请求split，通过此方法发送split给source reader
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param requesterHostname Optional, the hostname where the requesting task is running. This
     *     can be used to make split assignments locality-aware.
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (splits.isEmpty()) {
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.assignSplit(splits.remove(0), subtaskId);
        }
    }

    /**
     * 当某一个source reader异常退出时，会把split返回给enumerator，以便把该split分发给重新启动的新source reader
     * @param splits The splits to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<LogSplit> splits, int subtaskId) {
        this.splits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        System.out.printf("source reader %d added%n", subtaskId);
    }

    /**
     * 把未分发的split信息保存到checkpoint中
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return
     * @throws Exception
     */
    @Override
    public LogCheckpoint snapshotState(long checkpointId) throws Exception {
        return new LogCheckpoint(splits);
    }

    @Override
    public void close() throws IOException {
        System.out.println("split enumerator closed");
    }
}
