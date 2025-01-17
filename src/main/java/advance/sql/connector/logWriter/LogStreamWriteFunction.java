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
import java.util.List;
import java.util.StringJoiner;

public class LogStreamWriteFunction extends RichSinkFunction<RowData> {
    private String path;
    private final String separator;
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;

    public LogStreamWriteFunction(String path, String separator,
                                  List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.path = path;
        this.separator = separator;
        this.columnSchema = columnSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        int subtask = runtimeContext.getIndexOfThisSubtask();
        this.path += "-" + subtask;
    }

    /**
     * 需要sink的数据会通过invoke方法进行输出
     *
     * @param value   The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(RowData value, Context context) throws Exception {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path, true))) {
            bufferedWriter.write(transferRowDataToLine(value));
            bufferedWriter.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
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
}
