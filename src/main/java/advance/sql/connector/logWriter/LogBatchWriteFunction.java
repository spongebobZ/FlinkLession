package advance.sql.connector.logWriter;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class LogBatchWriteFunction extends RichSinkFunction<RowData> {
    private String path;
    private final String separator;
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;
    private final int batchSize;
    private List<String> buffer;

    public LogBatchWriteFunction(String path, String separator,
                                 List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema,
                                 int batchSize) {
        this.path = path;
        this.separator = separator;
        this.columnSchema = columnSchema;
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.buffer = new ArrayList<>(batchSize);
        RuntimeContext runtimeContext = getRuntimeContext();
        int subtask = runtimeContext.getIndexOfThisSubtask();
        this.path += "-" + subtask;
    }

    /**
     * 需要sink的数据会通过invoke方法进行输出
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // 注意不要直接把value存到buffer里，这里的value只是一个引用，当下一个value到达时会把引用的值改成新的value值
        buffer.add(transferRowDataToLine(value));
        if (buffer.size() == batchSize) {
            this.flush();
        }
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            this.flush();
        }
    }

    private String transferRowDataToLine(RowData rowData) {
        StringJoiner stringJoiner = new StringJoiner(separator);
        for (int i = 0; i < columnSchema.size(); i++) {
            stringJoiner.add(getStringValue(rowData, columnSchema.get(i), i));
        }
        return stringJoiner.toString();
    }

    private String getStringValue(RowData value, Tuple3<LogicalTypeRoot, Integer, Integer> schema, int valueIndex) {
        String v = null;
        if (!value.isNullAt(valueIndex)) {
            switch (schema.f0) {
                case DECIMAL:
                    v = value.getDecimal(valueIndex, schema.f1, schema.f2).toString();
                    break;
                case VARCHAR:
                case CHAR:
                    v = value.getString(valueIndex).toString();
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    v = value.getTimestamp(valueIndex, schema.f1).toTimestamp().toString();
                    break;
                case INTEGER:
                case DATE:
                    v = String.valueOf(value.getInt(valueIndex));
                    break;
                case BIGINT:
                    v = String.valueOf(value.getLong(valueIndex));
                    break;
                case SMALLINT:
                    v = String.valueOf(value.getShort(valueIndex));
                    break;
                default:
                    throw new IllegalArgumentException("unsupported column type: " + schema.f0.name());
            }
        }
        return v;
    }

    private void flush() {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path, true))) {
            for (String line : buffer) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            buffer.clear();
            throw new RuntimeException(e);
        }
        buffer.clear();
    }
}
