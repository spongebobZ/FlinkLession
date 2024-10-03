package conf;

import lombok.Getter;

public class MysqlConf {
    public static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String URL = "jdbc:mysql://localhost:3306/flink";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "Ab12345678#";
    public static final int FETCH_SIZE = 5000;
}
