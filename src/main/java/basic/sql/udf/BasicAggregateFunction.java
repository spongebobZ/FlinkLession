package basic.sql.udf;

import org.apache.flink.table.functions.AggregateFunction;


/**
 * 聚合函数
 * 1. 将一行或多行数据(可包含一个或多个字段)聚合统计成一个标量值
 * 2. 继承AggregateFunction类
 * 3. 重写createAccumulator方法获得初始累加器
 * 4. 定义accumulate方法，用于指定累加方式
 * 5. 可选，定义retract方法，用于指定故障恢复的数据撤回方式
 * 6. 可选，定义merge方法，用于指定多分区时的数据归并方式
 * 7. 可选，定义resetAccumulator方法，用于指定批处理场景下的重置累加器方式
 * 8. 重写getValue方法，从累加器中取得输出结果
 * 9. 先注册函数, 再调用函数
 */
public class BasicAggregateFunction extends AggregateFunction<Integer, RecordAcc> {
    /**
     * 为每个分区创建一个累加器，这里的累加器就是RecordAcc对象，RecordAcc对象将用来保存这个分区的该用户所有数据
     * @return 一个空的初始累加器
     */
    @Override
    public RecordAcc createAccumulator() {
        System.out.println("created an accumulator");
        return new RecordAcc();
    }

    /**
     * 定义聚合方式，当接收到一条新的数据时，通过这个聚合方式把这个数据聚合到累加器里
     * @param acc 累加器对象
     * @param income 新数据的收入字段
     * @param expense 新数据的支出字段
     */
    public void accumulate(RecordAcc acc, Integer income, Integer expense) {
        System.out.printf("got 1 row, income: %d, expense: %d%n", income, expense);
        int notnullIncome = income == null ? 0 : income;
        int notnullExpense = expense == null ? 0 : expense;
        acc.setTotalIncome(acc.getTotalIncome() + notnullIncome);
        acc.setTotalExpense(acc.getTotalExpense() + notnullExpense);
    }

    /**
     * optional方法。用于容错处理，当flink进行故障恢复时，需要把这条没有处理完的数据进行撤回，也就是把这条
     * 数据从累加器里移除，因此这个方法定义了移除的方式
     * @param acc 累加器对象
     * @param income 需要撤回的数据里的收入字段
     * @param expense 需要撤回的数据里的支出字段
     */
    public void retract(RecordAcc acc, Integer income, Integer expense) {
        int notnullIncome = income == null ? 0 : income;
        int notnullExpense = expense == null ? 0 : expense;
        acc.setTotalIncome(acc.getTotalIncome() - notnullIncome);
        acc.setTotalExpense(acc.getTotalExpense() - notnullExpense);
    }

    /**
     * optional方法。定义多个分区合并时的合并方式，在批处理场景和窗口聚合统计中需要实现,
     * 在这些场景中，会先在本分区进行数据预聚合，也就是说一个分组key的数据会在不同的分区预聚合(agg1)后再做一次聚合(agg2)到一起，
     * 这个merge方法就是给这个agg2步骤使用
     * @param acc 空的累加器对象
     * @param it 所有分区的累加器对象列表
     */
    public void merge(RecordAcc acc, Iterable<RecordAcc> it) {
        for (RecordAcc otherAcc : it) {
            acc.setTotalIncome(acc.getTotalIncome() + otherAcc.getTotalIncome());
            acc.setTotalExpense(acc.getTotalExpense() + otherAcc.getTotalExpense());
        }
    }

    /**
     * optional方法。定义重置累加器对象的方式，在批处理场景中需要实现
     * @param acc 累加器对象
     */
    public void resetAccumulator(RecordAcc acc) {
        acc.setTotalIncome(0);
        acc.setTotalExpense(0);
    }

    /**
     * 获取当前累加器结果的方法
     * @param acc 累加器对象
     * @return
     */
    @Override
    public Integer getValue(RecordAcc acc) {
        System.out.println("get value from acc: " + acc);
        return acc.getTotalIncome() - acc.getTotalExpense();
    }
}

