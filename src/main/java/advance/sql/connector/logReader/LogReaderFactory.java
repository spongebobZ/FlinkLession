package advance.sql.connector.logReader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.*;

/**
 * 作为源表，需要实现DynamicTableSourceFactory接口，
 * DynamicTableSourceFactory用于对接用户配置
 */
public class LogReaderFactory implements DynamicTableSourceFactory {
    // 读取模式
    public static final ConfigOption<String> MODE = ConfigOptions.key("mode")
            .stringType()
            .defaultValue("batch");

    // 日志文件路径
    public static final ConfigOption<String> PATH = ConfigOptions.key("path")
            .stringType()
            .noDefaultValue();

    // 分隔符
    public static final ConfigOption<String> SEPARATOR = ConfigOptions.key("separator")
            .stringType()
            .defaultValue(",");

    // 并行度
    public static final ConfigOption<Integer> PARALLELISM = ConfigOptions.key("parallelism")
            .intType()
            .defaultValue(1);

    /**
     * 通过重写createDynamicTableSource方法，返回DynamicTableSource对象
     * DynamicTableSource接口用于声明表运行时信息
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate require and optional options
        helper.validate();
        // extract options
        final ReadableConfig options = helper.getOptions();

        final List<DataType> physicalRowDataTypes = context.getCatalogTable().getResolvedSchema().getColumnDataTypes();
        final List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnsSchema = new ArrayList<>();
        final List<Column> columns = context.getCatalogTable().getResolvedSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (!columns.get(i).isPhysical()) {
                continue;
            }
            String dataType = physicalRowDataTypes.get(i).toString();
            Tuple2<Integer, Integer> ps = getDataTypePrecisionAndScale(dataType);
            columnsSchema.add(Tuple3.of(physicalRowDataTypes.get(i).getLogicalType().getTypeRoot(), ps.f0, ps.f1));
        }
        return new LogReaderSource(options.get(MODE), options.get(PATH), options.get(SEPARATOR), columnsSchema,
                options.get(PARALLELISM));
    }

    /**
     * 指定连接器名字
     */
    @Override
    public String factoryIdentifier() {
        return "log-reader";
    }

    /**
     * 指定连接器必选参数
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    /**
     * 指定连接器可选参数
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MODE);
        options.add(SEPARATOR);
        options.add(PARALLELISM);
        return options;
    }

    private Tuple2<Integer, Integer> getDataTypePrecisionAndScale(String dataType) {
        int precision = -1;
        int scale = -1;
        int openParenIdx = dataType.indexOf('(');
        if (openParenIdx == -1) {
            return Tuple2.of(precision, scale);
        }
        int closeParenIdx = dataType.indexOf(')');
        String[] ps = dataType.substring(openParenIdx + 1, closeParenIdx).split(",");
        precision = Integer.parseInt(ps[0]);
        if (ps.length > 1) {
            scale = Integer.parseInt(ps[1].trim());
        }
        return Tuple2.of(precision, scale);
    }
}
