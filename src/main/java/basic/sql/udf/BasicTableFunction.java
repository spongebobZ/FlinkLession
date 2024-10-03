package basic.sql.udf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 表函数
 * 1. 将输入标量值转换成一行或多行数据(可包含一个或多个字段)
 * 2. 继承TableFunction类
 * 3. 使用FunctionHint注解声明返回的每个字段名和字段类型
 * 4. 使用无返回值的eval方法接收参数并处理，在方法里使用collect()方法输出结果数据，每次调用collect()方法输出一行数据
 * 5. 先注册函数, 再调用函数
 */
@FunctionHint(output = @DataTypeHint("row<income int, expense int>"))
public class BasicTableFunction extends TableFunction<Row> {
    // 保存每个用户每天的收入和支出
    private final List<Record> records = Arrays.asList(
            new Record(1, 100, 20),
            new Record(1, 200, 50),
            new Record(2, 50, null),
            new Record(3, null, 100)
    );

    /**
     * 根据uid参数查询所有自然日的收入和支出记录
     * @param uid
     * 返回一个Row对象，表示一行，包含了两个字段
     */
    public void eval(int uid) {
        for (Record record : records) {
            if (record.getUid() == uid) {
                collect(Row.of(record.getIncome(), record.getExpense()));
            }
        }
    }

    @Getter
    @AllArgsConstructor
    public static class Record implements Serializable {
        private int uid;
        private Integer income;
        private Integer expense;
    }
}
