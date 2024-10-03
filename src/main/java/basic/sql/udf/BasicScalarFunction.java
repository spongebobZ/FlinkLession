package basic.sql.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 标量函数
 * 1. 将输入标量值转换成一个新标量值
 * 2. 继承ScalarFunction类
 * 3. 使用eval方法接收参数并处理
 * 4. 先注册函数, 再调用函数
 */
public class BasicScalarFunction extends ScalarFunction {
    /**
     * 接收两个整数, 返回它们的差. eval方法必须用public修饰，返回类型根据需要来定
     * @param a 参数1，可为null
     * @param b 参数2，可为null
     * @return 参数a减参数b的差，参数null值按0处理
     */
    public int eval(Integer a, Integer b) {
        int notnullA = a == null ? 0 : a;
        int notnullB = b == null ? 0 : b;
        return notnullA - notnullB;
    }
}
