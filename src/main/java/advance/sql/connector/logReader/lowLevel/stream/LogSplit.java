package advance.sql.connector.logReader.lowLevel.stream;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * split分片，可以用来存储数据源也可以存储数据源读取标识
 * 存储数据源会导致分片管理器的内存占用过高，并且网络传输压力大
 */
public class LogSplit implements SourceSplit {
    private final String splitId;
    private final int workerCount;
    private final int currentWorkerId;
    private int currentRowNum = 0;
    private boolean snapshotFinished = false;


    /**
     * 用于新建split
     * @param workerCount
     * @param currentWorkerId
     */
    public LogSplit(int workerCount, int currentWorkerId) {
        this.workerCount = workerCount;
        this.currentWorkerId = currentWorkerId;
        this.splitId = String.valueOf(currentWorkerId);
    }

    /**
     * 用于反序列化split
     * @param workerCount
     * @param currentWorkerId
     * @param currentRowNum
     * @param snapshotFinished
     */
    public LogSplit(String splitId, int workerCount, int currentWorkerId, int currentRowNum, boolean snapshotFinished) {
        this.workerCount = workerCount;
        this.currentWorkerId = currentWorkerId;
        this.splitId = splitId;
        this.currentRowNum = currentRowNum;
        this.snapshotFinished = snapshotFinished;
    }
    @Override
    public String splitId() {
        return this.splitId;
    }

    public int getWorkerCount() {
        return this.workerCount;
    }

    public int getCurrentWorkerId() {
        return this.currentWorkerId;
    }

    public int getCurrentRowNum() {
        return this.currentRowNum;
    }

    public void increaseCurrentRowNum() {
        this.currentRowNum += 1;
    }

    public boolean isSnapshotFinished() {
        return this.snapshotFinished;
    }

    public void setSnapshotFinished() {
        this.snapshotFinished = true;
    }
}
