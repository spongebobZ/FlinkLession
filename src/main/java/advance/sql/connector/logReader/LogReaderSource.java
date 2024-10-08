package advance.sql.connector.logReader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import java.util.*;

/**
 * 实现ScanTableSource接口，而ScanTableSource接口是DynamicTableSource的子类
 * 用于声明表运行时信息
 */
public class LogReaderSource implements ScanTableSource, SupportsFilterPushDown {
    private final String mode;
    private final String path;
    private final String separator;
    private final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema;
    private final int parallelism;
    private final Map<Integer, Tuple2<LogicalTypeRoot, Object>> predicates = new HashMap<>();
    public LogReaderSource(String mode, String path, String separator,
                           List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema, int parallelism) {
        this.mode = mode;
        this.path = path;
        this.separator = separator;
        this.columnSchema = columnSchema;
        this.parallelism = parallelism;
    }



    /**
     * 复制该对象的方法，在运行时存在复制到多个线程里去的需求
     */
    @Override
    public DynamicTableSource copy() {
        return new LogReaderSource(mode, path, separator, columnSchema, parallelism);
    }

    /**
     * 连接器描述
     */
    @Override
    public String asSummaryString() {
        return "log reader";
    }

    /**
     * 连接器输入数据的op类型
     */
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    /**
     * 重写getScanRuntimeProvider方法，返回ScanRuntimeProvider
     * ScanRuntimeProvider用于进行从数据源读取数据的行为
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (mode.equalsIgnoreCase("batch")) {
            return InputFormatProvider.of(new LogReaderBatchParallel(path, separator, columnSchema, parallelism, predicates));
        } else {
            return InputFormatProvider.of(new LogReaderStreamParallel(path, separator, columnSchema, parallelism));
        }
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        for (ResolvedExpression expression: filters) {
            CallExpression callExpression = (CallExpression) expression;
            FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
            // 只处理等值条件
            if (functionDefinition != BuiltInFunctionDefinitions.EQUALS) {
                remainingFilters.add(expression);
                continue;
            }
            FieldReferenceExpression fieldReferenceExpression =
                    (FieldReferenceExpression) callExpression.getChildren().get(0);
            int idx = fieldReferenceExpression.getFieldIndex(); // 过滤字段在表结构中的index
            LogicalTypeRoot logicalTypeRoot = fieldReferenceExpression.getOutputDataType().getLogicalType().getTypeRoot();
            switch (logicalTypeRoot) {
                case VARCHAR:
                case CHAR:
                case INTEGER:
                    acceptedFilters.add(expression);
                    break;
                default:
                    remainingFilters.add(expression);
                    continue;
            }
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) callExpression.getChildren().get(1); // 过滤字段值
            Object value = valueLiteralExpression.getValueAs(Object.class).orElse(null);
            predicates.put(idx, Tuple2.of(logicalTypeRoot, value));
        }
        return Result.of(acceptedFilters, remainingFilters);
    }
}
