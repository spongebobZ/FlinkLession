import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;

public class FileTest {
    public static void main(String[] args) {
        String path = "/Users/jolin/Documents/codes/Flink/src/main/java/advance/sql/connector/logReader/ods_score.csv";
        try (LineIterator lineIterator = FileUtils.lineIterator(new File(path), "UTF-8")) {
            // 一旦监听到数据发生变更，则从position位置往后读取所有行
            while (lineIterator.hasNext()) {
                Thread.sleep(2000L);
                String line = lineIterator.nextLine();
                // 每次都需要从第一行开始扫描，扫描到position记录的位置后才开始处理行数据
                System.out.println(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
