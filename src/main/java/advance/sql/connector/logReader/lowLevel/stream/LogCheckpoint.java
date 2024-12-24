package advance.sql.connector.logReader.lowLevel.stream;

import java.io.Serializable;
import java.util.Map;

public class LogCheckpoint implements Serializable {
    private final Map<Integer, LogSplit> splitMap;

    public LogCheckpoint(Map<Integer, LogSplit> splitMap) {
        this.splitMap = splitMap;
    }

    public Map<Integer, LogSplit> getSplitMap() {
        return this.splitMap;
    }
}
