package advance.sql.connector.logReader.lowLevel.batch;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.UUID;

/**
 * split分片，可以用来存储数据源也可以存储数据源读取标识
 * 存储数据源会导致分片管理器的内存占用过高，并且网络传输压力大
 */
public class LogSplit implements SourceSplit {
    private final String splitId;
    private final String filePath;
    private final int lowRowNum;
    private final int highRowNum;


    /**
     * 用于新建split
     */
    public LogSplit(String filePath, int lowRowNum, int highRowNum) {
        this.filePath = filePath;
        this.lowRowNum = lowRowNum;
        this.highRowNum = highRowNum;
        this.splitId = UUID.randomUUID().toString();
    }

    /**
     * 用于反序列化split
     */
    public LogSplit(String splitId, String filePath, int lowRowNum, int highRowNum) {
        this.splitId = splitId;
        this.filePath = filePath;
        this.lowRowNum = lowRowNum;
        this.highRowNum = highRowNum;
    }
    @Override
    public String splitId() {
        return this.splitId;
    }

    public String getFilePath() {
        return this.filePath;
    }
    
    public int getLowRowNum() {
        return this.lowRowNum;
    }

    public int getHighRowNum() {
        return this.highRowNum;
    }
}
