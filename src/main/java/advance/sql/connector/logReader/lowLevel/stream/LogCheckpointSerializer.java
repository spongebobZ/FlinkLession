package advance.sql.connector.logReader.lowLevel.stream;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class LogCheckpointSerializer implements SimpleVersionedSerializer<LogCheckpoint> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(LogCheckpoint logCheckpoint) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            Map<Integer, LogSplit> splitMap = logCheckpoint.getSplitMap();
            dos.writeInt(splitMap.size());
            for (Map.Entry<Integer, LogSplit> entry : splitMap.entrySet()) {
                dos.writeInt(entry.getKey());
                LogSplit split = entry.getValue();
                dos.writeUTF(split.splitId());
                dos.writeInt(split.getWorkerCount());
                dos.writeInt(split.getCurrentWorkerId());
                dos.writeInt(split.getCurrentRowNum());
                dos.writeBoolean(split.isSnapshotFinished());
            }
            return baos.toByteArray();
        }
    }

    @Override
    public LogCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
             DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
            int size = dataInputStream.readInt();
            Map<Integer, LogSplit> splitMap = new HashMap<>(size);
            for (int i=0;i< size;i++) {
                int workerId = dataInputStream.readInt();
                String splitId = dataInputStream.readUTF();
                int workerCount = dataInputStream.readInt();
                int currentWorkerId = dataInputStream.readInt();
                int currentRowNum = dataInputStream.readInt();
                boolean snapshotFinished = dataInputStream.readBoolean();
                LogSplit logSplit = new LogSplit(splitId, workerCount, currentWorkerId, currentRowNum, snapshotFinished);
                splitMap.put(workerId, logSplit);
            }
            return new LogCheckpoint(splitMap);
        }
    }
}
