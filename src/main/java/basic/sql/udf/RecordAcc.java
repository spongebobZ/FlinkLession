package basic.sql.udf;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
public class RecordAcc {
    private Integer totalIncome;
    private Integer totalExpense;

    public RecordAcc() {
        this.totalIncome = 0;
        this.totalExpense = 0;
    }

    @Override
    public String toString() {
        return String.format("{accId: %d, income: %d, expense: %d}",
                this.hashCode(), totalIncome, totalExpense);
    }
}
