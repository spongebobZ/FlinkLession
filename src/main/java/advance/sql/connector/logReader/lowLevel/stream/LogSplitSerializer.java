package advance.sql.connector.logReader.lowLevel.stream;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class LogSplitSerializer implements SimpleVersionedSerializer<LogSplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    /**
     * 把恢复split需要的字段信息序列化进去
     * @param split The object to serialize.
     * @return
     * @throws IOException
     */
    @Override
    public byte[] serialize(LogSplit split) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            dataOutputStream.writeUTF(split.splitId());
            dataOutputStream.writeInt(split.getWorkerCount());
            dataOutputStream.writeInt(split.getCurrentWorkerId());
            dataOutputStream.writeInt(split.getCurrentRowNum());
            dataOutputStream.writeBoolean(split.isSnapshotFinished());
            return byteArrayOutputStream.toByteArray();
        }
    }

    /**
     * 对序列化二进制数据进行反序列化，得到split
     * @param version The version in which the data was serialized
     * @param serialized The serialized data
     * @return
     * @throws IOException
     */
    @Override
    public LogSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
             DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)){
            String splitId = dataInputStream.readUTF();
            int workerCount = dataInputStream.readInt();
            int currentWorkerId = dataInputStream.readInt();
            int currentRowNum = dataInputStream.readInt();
            boolean snapshotFinished = dataInputStream.readBoolean();
            return new LogSplit(splitId, workerCount, currentWorkerId, currentRowNum, snapshotFinished);
        }
    }
}
