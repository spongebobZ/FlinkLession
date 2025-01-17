package advance.sql.connector.logReader.lowLevel.stream;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogEnumerator implements SplitEnumerator<LogSplit, LogCheckpoint> {
    private final SplitEnumeratorContext<LogSplit> context;
    private int parallelism;
    private Map<Integer, LogSplit> splitMap;

    /**
     * 用于新建LogEnumerator
     */
    public LogEnumerator(SplitEnumeratorContext<LogSplit> context, int parallelism) {
        this.context = context;
        this.parallelism = parallelism;
    }

    /**
     * 用于从checkpoint数据中恢复LogEnumerator
     */
    public LogEnumerator(SplitEnumeratorContext<LogSplit> context, Map<Integer, LogSplit> splitMap){
        this.context = context;
        this.splitMap = splitMap;
    }



    /**
     * 生成所有分区的split
     */
    @Override
    public void start() {
        this.splitMap = new HashMap<>(parallelism);
        for (int i=0;i<parallelism;i++) {
            LogSplit logSplit = new LogSplit(parallelism, i);
            splitMap.put(i, logSplit);
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
        context.assignSplit(splitMap.get(subtaskId), subtaskId);
    }

    /**
     * 当某一个source reader异常退出时，会把split返回给enumerator，以便把该split分发给重新启动的新source reader
     * @param splits The splits to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<LogSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            splitMap.put(subtaskId, splits.get(0));
        }
    }

    @Override
    public void addReader(int subtaskId) {
    }

    /**
     * 把未分发的split信息保存到checkpoint中
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return
     * @throws Exception
     */
    @Override
    public LogCheckpoint snapshotState(long checkpointId) throws Exception {
        return new LogCheckpoint(splitMap);
    }

    @Override
    public void close() throws IOException {
    }
}
