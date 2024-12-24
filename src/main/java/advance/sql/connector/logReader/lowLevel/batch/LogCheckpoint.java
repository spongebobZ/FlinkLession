package advance.sql.connector.logReader.lowLevel.batch;

import java.io.Serializable;
import java.util.List;

/**
 * 1.用来持久化enumerator端未分配的splits信息
 * 2.用来持久化source reader端未消费的splits信息
 */
public class LogCheckpoint implements Serializable {
    private final List<LogSplit> splits;

    public LogCheckpoint(List<LogSplit> splits) {
        this.splits = splits;
    }

    public List<LogSplit> getSplits() {
        return this.splits;
    }
}
