import java.math.BigDecimal;

public class BigDecimalTest {
    public static void main(String[] args) {
        BigDecimal a = new BigDecimal("1.0");
        BigDecimal b = null;
        System.out.println(a.compareTo(b) == 0);
    }
}
