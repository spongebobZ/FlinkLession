package advance.sql.connector.logReader.lowLevel.batch;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LogCheckpointSerializer implements SimpleVersionedSerializer<LogCheckpoint> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(LogCheckpoint logCheckpoint) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            List<LogSplit> splits = logCheckpoint.getSplits();
            dos.writeInt(splits.size());
            for (LogSplit split: splits) {
                dos.writeUTF(split.splitId());
                dos.writeUTF(split.getFilePath());
                dos.writeInt(split.getLowRowNum());
                dos.writeInt(split.getHighRowNum());
            }
            return baos.toByteArray();
        }
    }

    @Override
    public LogCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
             DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
            int size = dataInputStream.readInt();
            List<LogSplit> splits = new ArrayList<>(size);
            for (int i=0;i< size;i++) {
                String splitId = dataInputStream.readUTF();
                String filePath = dataInputStream.readUTF();
                int lowRowNum = dataInputStream.readInt();
                int highRowNum = dataInputStream.readInt();
                LogSplit logSplit = new LogSplit(splitId, filePath, lowRowNum, highRowNum);
                splits.add(logSplit);
            }
            return new LogCheckpoint(splits);
        }
    }
}
