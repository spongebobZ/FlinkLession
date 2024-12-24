package advance.sql.connector.logReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.planner.functions.inference.LookupCallContext;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.utils.AdaptedCallContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

@Slf4j
public class LogReaderLookupFunction extends LookupFunction {
    private final String path;
    private final DeserializeFormatter deserializeFormatter;
    private int[] compareIndices;
    private int compareSize;

    public LogReaderLookupFunction(String path, String separator,
                                   List<Tuple3<LogicalTypeRoot, Integer, Integer>> columnSchema) {
        this.path = path;
        this.deserializeFormatter = new DeserializeFormatter(columnSchema, separator);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException { // +I(1)
        List<RowData> results = new ArrayList<>();
        try (LineIterator lineIterator = FileUtils.lineIterator(new File(this.path), "UTF-8")) {
            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                GenericRowData rowData = deserializeFormatter.deserializeToRowData(line); // +I(1,Jenny,18)
                int i = 0;
                GenericRowData genericKeyRow = (GenericRowData) keyRow;
                while (i < compareSize) { //[0,2]
                    if (Objects.equals(rowData.getField(compareIndices[i]), genericKeyRow.getField(i))) {
                        i++;
                        continue;
                    }
                    break;
                }
                if (i == compareSize) {
                    results.add(rowData);
                }
            }
        } catch (IOException e) {
            log.error("read file error, {}", e.getMessage());
        }
        return results;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(callContext -> {
                    AdaptedCallContext adaptedCallContext = (AdaptedCallContext) callContext;
                    try {
                        Field field = adaptedCallContext.getClass().getDeclaredField("originalContext");
                        field.setAccessible(true);
                        CallContext context1 = (CallContext) field.get(adaptedCallContext);
                        LookupCallContext lookupCallContext = (LookupCallContext) context1;
                        field = lookupCallContext.getClass().getDeclaredField("lookupKeys");
                        field.setAccessible(true);
                        Map<Integer, LookupJoinUtil.LookupKey> lookupKeys = (Map<Integer, LookupJoinUtil.LookupKey>) field.get(lookupCallContext);
                        this.compareIndices = new int[lookupKeys.size()];
                        int i = 0;
                        for (int idx : lookupKeys.keySet()) {
                            this.compareIndices[i] = idx;
                            i++;
                        }
                        this.compareSize = this.compareIndices.length;
                        return Optional.ofNullable(lookupCallContext.getOutputDataType().get().toInternal());
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).build();
    }
}
