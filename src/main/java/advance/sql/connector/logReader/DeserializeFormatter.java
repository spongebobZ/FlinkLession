package advance.sql.connector.logReader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

/**
 * 提供了把字符串文本转换为RowData的方法
 */
public class DeserializeFormatter implements Serializable {
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;
    private final String separator;
    private final int columnSize;

    public DeserializeFormatter(List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema,
                                String separator) {
        this.columnSchema = columnSchema;
        this.separator = separator;
        this.columnSize = columnSchema.size();
    }

    public GenericRowData deserializeToRowData(String line) {
        String[] arr = line.split(separator);
        if (arr.length != columnSize) {
            return null;
        }
        GenericRowData genericRowData = new GenericRowData(RowKind.INSERT, columnSize);
        for (int i = 0; i < columnSize; i++) {
            Object formatterValue = deserialize(arr[i], i);
            genericRowData.setField(i, formatterValue);
        }
        return genericRowData;
    }

    private Object deserialize(String originValue, int index) {
        if (originValue == null || originValue.isEmpty()) {
            return null;
        }
        Tuple3<LogicalTypeRoot, Integer, Integer> schema = columnSchema.get(index);
        switch (schema.f0) {
            case INTEGER:
            case DATE:
                return Integer.parseInt(originValue);
            case DECIMAL:
                return DecimalData.fromBigDecimal(new BigDecimal(originValue), schema.f1, schema.f2);
            case VARCHAR:
                return StringData.fromString(originValue);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromTimestamp(Timestamp.valueOf(originValue));
            case SMALLINT:
                return Short.parseShort(originValue);
            case BIGINT:
                return Long.parseLong(originValue);
            default:
                throw new IllegalArgumentException("unsupported dataType: " + schema.f0.name());
        }
    }
}
